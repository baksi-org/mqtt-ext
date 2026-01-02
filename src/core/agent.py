import os
import asyncio
import logging
import signal
import time
import queue
from asyncio import Task
from threading import Thread
from typing import Any, Dict, List, Optional, Callable
from multiprocessing import Process, Queue, Event

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from core.models import MQTTInput, SignalVariable
from core.workers import consumer_worker_entry, task_bridge
from utils import get_config, get_custom_config
from .forms import dynamic_mqtt_form, form_dict_to_input

from plotune_sdk import PlotuneRuntime
from plotune_sdk.models import Variable

logger = logging.getLogger("mqtt_agent")
logger.setLevel(logging.INFO)


class MQTTAgent:
    def __init__(self):
        self.config = get_config()
        self.custom_config = get_custom_config()

        self.memory: Dict[str, SignalVariable] = {}
        self.stream_handler: Dict[str, List[tuple]] = {}

        self._runtime: Optional[PlotuneRuntime] = None
        self._api: Optional[FastAPI] = None

        self.data_queue: List[Queue] = []
        self.new_topic_queue = Queue()
        self._stop_event = Event()

        self.tasks: List[Task] = []
        self.processes: List[Process] = []

        self.bridges: Dict[str, Task] = {}
        self.producer_conf: Optional[MQTTInput] = None
        self.producer_client = None

        self._agent_loop: Optional[asyncio.AbstractEventLoop] = None
        self._agent_thread: Optional[Thread] = None

        # runtime-side push queue (thread-safe), created during runtime startup
        self._runtime_push_queue: Optional[queue.Queue] = None

        self._register_events()

    def _register_events(self) -> None:
        server = self.runtime.server
        server.on_event("/form")(self._handle_form)
        server.on_event("/fetch-meta")(self.fetch_signals)
        server.on_event("/form", method="POST")(self._new_connection)
        server.on_event("/bridge/{variable_name}", method="POST")(self.on_bridge)
        server.on_event("/unbridge/{variable_name}", method="POST")(self.on_unbridge)

        # runtime.server may register websockets; SDK should use "/fetch/{signal_name:path}"
        server.on_ws()(self.stream)

    async def on_bridge(self, variable: Variable):
        if not self.producer_conf:
            try:
                await self.runtime.core_client.toast(
                    "MQTT Extension", "No producer data source added", duration=5000
                )
            except Exception:
                logger.warning("Toast failed (runtime not ready)")
            logger.error("Producer configuration is not defined by user")
            return

        name = variable.name
        if name in self.bridges:
            return

        task = asyncio.create_task(
            task_bridge(variable, self.producer_conf, self._stop_event)
        )
        self.bridges[name] = task

    async def on_unbridge(self, variable: Variable):
        name = variable.name
        t = self.bridges.pop(name, None)
        if t:
            t.cancel()

    async def fetch_signals(self, data: dict) -> Dict[str, List[str]]:
        return {"headers": []}

    async def _handle_form(self, data: dict) -> Any:
        return dynamic_mqtt_form()

    async def _new_connection(self, data: dict) -> Dict[str, str]:
        form: MQTTInput = form_dict_to_input(data)

        if form.producer:
            self.producer_conf = form

        data_queue = Queue()
        proc = Process(
            target=consumer_worker_entry,
            args=(data, data_queue, self.new_topic_queue, self._stop_event),
            daemon=True,
        )
        proc.start()

        self.data_queue.append(data_queue)
        self.processes.append(proc)

        if not self._agent_loop:
            self._start_agent_loop()

        asyncio.run_coroutine_threadsafe(
            self.handle_data_queue(data_queue), self._agent_loop
        )

        return {"status": "success", "message": "Form saved!"}

    def handle_sigint(self, sig, frame):
        self._stop_event.set()
        # notify runtime push worker to stop
        if self._runtime_push_queue is not None:
            try:
                self._runtime_push_queue.put_nowait(None)
            except Exception:
                pass

        for p in self.processes:
            try:
                p.join(2)
                if p.is_alive():
                    p.terminate()
                    p.join(2)
            except Exception:
                logger.exception("Error stopping worker process")

    async def _mp_queue_get(self, mp_queue: Queue, timeout: float = 1.0):
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(None, mp_queue.get, True, timeout)
        except Exception:
            return None

    # runtime-side worker: runs on runtime event loop, drains thread-safe queue and calls add_variable
    async def _runtime_push_worker(self):
        loop = asyncio.get_running_loop()
        q = self._runtime_push_queue
        if q is None:
            return

        while True:
            # blocking get in threadpool, returns None sentinel to exit
            item = await loop.run_in_executor(None, q.get)
            if item is None:
                return
            topic, timestamp = item
            try:
                await self.runtime.core_client.add_variable(
                    variable_name=topic, variable_desc=str(timestamp)
                )
                logger.info("Added variable for topic %s", topic)
            except Exception:
                logger.exception("Failed to add variable for topic %s", topic)

    async def handle_new_signal(self):
        while not self._stop_event.is_set():
            msg = await self._mp_queue_get(self.new_topic_queue, timeout=1.0)
            if not msg:
                continue
            raw_topic = msg.get("topic")
            if raw_topic is None:
                continue
            topic_key = self._topic_to_key(raw_topic)
            timestamp = msg.get("timestamp", "No information")

            # push into runtime-side thread-safe queue (created on runtime startup)
            if self._runtime_push_queue is not None:
                try:
                    self._runtime_push_queue.put_nowait((topic_key, timestamp))
                except Exception:
                    logger.exception(
                        "Failed to push new topic to runtime queue: %s", topic_key
                    )
            else:
                # runtime not started yet -> best-effort schedule using current loop (may fail)
                try:
                    coro = self.runtime.core_client.add_variable(
                        variable_name=topic_key, variable_desc=str(timestamp)
                    )
                    try:
                        asyncio.ensure_future(coro)
                        logger.warning(
                            "Scheduled add_variable via ensure_future (runtime loop may differ). topic=%s",
                            topic_key,
                        )
                    except Exception:
                        logger.exception("ensure_future failed for topic %s", topic_key)
                except Exception:
                    logger.exception(
                        "Failed to call add_variable directly for %s", topic_key
                    )

    async def handle_data_queue(self, data_q: Queue):
        while not self._stop_event.is_set():
            msg = await self._mp_queue_get(data_q, timeout=1.0)
            if not msg:
                continue
            try:
                raw_topic = msg.get("topic")
                if raw_topic is None:
                    continue

                key = self._topic_to_key(raw_topic)
                raw_payload = msg.get("payload")
                ts = msg.get("timestamp", time.time())

                if isinstance(raw_payload, dict):
                    value = raw_payload.get(
                        "value", raw_payload.get("val", raw_payload)
                    )
                    when = raw_payload.get("time", raw_payload.get("timestamp", ts))
                else:
                    value = raw_payload
                    when = ts

                normalized = {"value": value, "time": when}

                entry = self.memory.get(key)
                if not entry:
                    entry = SignalVariable(name=key)
                    self.memory[key] = entry
                entry.payload.append(normalized)

                handlers = self.stream_handler.get(key)
                if not handlers:
                    continue

                for cb, cb_loop in list(handlers):
                    try:
                        asyncio.run_coroutine_threadsafe(cb(normalized), cb_loop)
                    except Exception:
                        logger.exception(
                            "Failed to schedule websocket callback for topic %s",
                            raw_topic,
                        )

            except Exception:
                logger.exception("Error processing message from data queue")

    async def stream(self, signal_name: str, websocket: WebSocket, data: Any) -> None:
        key = signal_name
        topic = self._key_to_topic(key)

        entry = self.memory.get(key)
        if entry:
            for payload in entry.payload:
                await websocket.send_json(
                    {"timestamp": payload["time"], "value": payload["value"]}
                )

        current_loop = asyncio.get_running_loop()

        async def send_payload(payload: dict):
            await websocket.send_json(
                {"timestamp": payload["time"], "value": payload["value"]}
            )

        self.stream_handler.setdefault(key, []).append((send_payload, current_loop))

        try:
            while True:
                await websocket.receive_text()
        except (WebSocketDisconnect, Exception):
            pass
        finally:
            handlers = self.stream_handler.get(key, [])
            for h in list(handlers):
                if h[1] is current_loop and getattr(h[0], "__name__", None) == getattr(
                    send_payload, "__name__", None
                ):
                    handlers.remove(h)
            if not handlers:
                self.stream_handler.pop(key, None)
            await websocket.close()

    def _topic_to_key(self, topic: str) -> str:
        return topic.replace("/", "_") if topic else topic

    def _key_to_topic(self, key: str) -> str:
        return key.replace("_", "/") if key else key

    @property
    def api(self) -> FastAPI:
        if not self._api:
            self._api = self.runtime.server.api
        return self._api

    @property
    def runtime(self) -> PlotuneRuntime:
        if self._runtime:
            return self._runtime

        connection = self.config.get("connection", {})
        target = connection.get("target", "127.0.0.1")
        port = connection.get("target_port", "8000")
        core_url = f"http://{target}:{port}"

        self._runtime = PlotuneRuntime(
            ext_name=self.config.get("id"),
            core_url=core_url,
            config=self.config,
        )

        return self._runtime

    def _start_agent_loop(self):
        if self._agent_loop and self._agent_loop.is_running():
            return

        def _run():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._agent_loop = loop
            loop.create_task(self.handle_new_signal())
            try:
                loop.run_forever()
            finally:
                pending = asyncio.all_tasks(loop=loop)
                for t in pending:
                    t.cancel()
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.close()

        t = Thread(target=_run, daemon=True, name="mqtt-agent-loop")
        t.start()
        while self._agent_loop is None:
            time.sleep(0.01)
        self._agent_thread = t

    def start(self) -> None:
        signal.signal(signal.SIGINT, self.handle_sigint)
        signal.signal(signal.SIGTERM, self.handle_sigint)

        # register runtime-side startup/shutdown handlers on the FastAPI app so runtime worker runs on runtime loop
        app = self.runtime.server.api

        @app.on_event("startup")
        async def _runtime_startup():
            self._runtime_push_queue = queue.Queue()
            asyncio.create_task(self._runtime_push_worker())

        @app.on_event("shutdown")
        async def _runtime_shutdown():
            if self._runtime_push_queue is not None:
                try:
                    self._runtime_push_queue.put_nowait(None)
                except Exception:
                    pass

        # start agent loop (handles multiprocessing queues)
        if not self._agent_loop:
            self._start_agent_loop()

        # finally start runtime (blocking)
        self.runtime.start()
