from plotune_sdk import PlotuneRuntime
from plotune_sdk.models import FileMetaData, FileReadRequest
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
import os
import asyncio
import logging
import signal
import time
from threading import Thread
from typing import Any, Dict, List, Optional, Callable
from asyncio import Task
from multiprocessing import Process, Queue, Event
from core.models import MQTTInput, Variable
from core.workers import consumer_worker_entry
from utils import get_config, get_custom_config
from .forms import dynamic_mqtt_form, form_dict_to_input

logger = logging.getLogger(__name__)

class MQTTAgent:
    def __init__(self):
        self.config = get_config()
        self.custom_config = get_custom_config()

        self.memory:Dict[str, Variable] = {}
        self.stream_handler:Dict[str, List[Callable]] = {}

        self._runtime: Optional[PlotuneRuntime] = None
        self._api: Optional[FastAPI] = None
        self.data_queue: List[Queue] = []
        self.new_topic_queue = Queue()
        self._stop_event = Event()

        self.tasks: List[Task] = []
        self.processes: List[Process] = []

        self._agent_loop: Optional[asyncio.AbstractEventLoop] = None
        self._agent_thread: Optional[Thread] = None

        self._register_events()

    def _register_events(self) -> None:
        server = self.runtime.server
        server.on_event("/form")(self._handle_form)
        server.on_event("/fetch-meta")(self.fetch_signals)
        server.on_event("/form", method="POST")(self._new_connection)
        server.on_ws()(self.stream)
        logger.debug("Runtime events registered")

    async def fetch_signals(self, data: dict) -> Dict[str, List[str]]:
        return {"headers": []}

    async def _handle_form(self, data: dict) -> Any:
        logger.debug("Dynamic relay form requested")
        return dynamic_mqtt_form()

    async def _new_connection(self, data: dict) -> Dict[str, str]:
        form: MQTTInput = form_dict_to_input(data)

        data_queue = Queue()
        proc = Process(
            target=consumer_worker_entry,
            args=(data, data_queue, self.new_topic_queue, self._stop_event),
            daemon=True
        )
        proc.start()

        self.data_queue.append(data_queue)
        self.processes.append(proc)

        if not self._agent_loop:
            self._start_agent_loop()

        asyncio.run_coroutine_threadsafe(self.handle_data_queue(data_queue), self._agent_loop)

        return {"status": "success", "message": "Form saved!"}

    def handle_sigint(self, sig, frame):
        self._stop_event.set()
        for _process in self.processes:
            _process.join(2)
            if _process.is_alive():
                _process.terminate()
                _process.join(2)

    async def _mp_queue_get(self, mp_queue: Queue, timeout: float = 1.0):
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(None, mp_queue.get, True, timeout)
        except Exception:
            return None

    async def handle_new_signal(self):
        while not self._stop_event.is_set():
            msg = await self._mp_queue_get(self.new_topic_queue, timeout=1.0)
            if not msg:
                continue
            topic = msg.get("topic").replace("/","_")
            _timestamp = msg.get("timestamp", "No information")
            if not topic:
                continue

            try:
                coro = self.runtime.core_client.add_variable(
                    variable_name=topic,
                    variable_desc=f"{_timestamp}",
                )

                # try to locate a "runtime" loop; fall back to captured main loop
                target_loop = None
                for attr in ("_loop", "loop", "event_loop", "_event_loop"):
                    maybe = getattr(self.runtime, attr, None)
                    if isinstance(maybe, asyncio.AbstractEventLoop):
                        target_loop = maybe
                        break

                if target_loop is None:
                    target_loop = self._main_loop

                if target_loop is None:
                    # last resort: schedule on current loop but don't await (may fail)
                    fut = asyncio.ensure_future(coro)
                    def _cb(f):
                        if f.exception():
                            logger.exception("add_variable failed (ensure_future) for %s", topic)
                    fut.add_done_callback(_cb)
                else:
                    fut = asyncio.run_coroutine_threadsafe(coro, target_loop)
                    def _done(future):
                        try:
                            future.result()  # trigger exception if any for logging
                            logger.info("Added variable for topic %s", topic)
                        except Exception:
                            logger.exception("Failed to add variable for topic %s", topic)
                    fut.add_done_callback(_done)

            except Exception:
                logger.exception("Error scheduling add_variable for topic %s", topic)

    async def handle_data_queue(self, data_q: Queue):
        while not self._stop_event.is_set():
            msg = await self._mp_queue_get(data_q, timeout=1.0)
            if not msg:
                continue
            try:
                topic = msg.get("topic")
                raw_payload = msg.get("payload")
                ts = msg.get("timestamp", time.time())

                # normalize payload -> {"value": ..., "time": ...}
                if isinstance(raw_payload, dict):
                    value = raw_payload.get("value", raw_payload.get("val", raw_payload))
                    when = raw_payload.get("time", raw_payload.get("timestamp", ts))
                else:
                    value = raw_payload
                    when = ts

                normalized = {"value": value, "time": when}

                # ensure memory structure
                entry = self.memory.get(topic)
                if not entry:
                    entry = {"payload": []}
                    self.memory[topic] = entry
                entry["payload"].append(normalized)

                handlers = self.stream_handler.get(topic)
                if not handlers:
                    continue

                # schedule delivery on each handler's loop
                for cb, cb_loop in list(handlers):
                    try:
                        asyncio.run_coroutine_threadsafe(cb(normalized), cb_loop)
                    except Exception:
                        logger.exception("Failed to schedule websocket callback for topic %s", topic)

            except Exception:
                logger.exception("Error processing message from data queue")

    async def stream(
        self,
        signal_name: str,
        websocket: WebSocket,
        data: Any,
    ) -> None:
        topic = signal_name.replace("_","/")
        
        logger.info("Client requested topic '%s'", topic)

        entry = self.memory.get(topic)
        if entry:
            for payload in entry["payload"]:
                await websocket.send_json({"timestamp": payload["time"], "value": payload["value"]})

        current_loop = asyncio.get_running_loop()

        async def send_payload(payload: dict):
            await websocket.send_json({"timestamp": payload["time"], "value": payload["value"]})

        self.stream_handler.setdefault(topic, []).append((send_payload, current_loop))

        try:
            while True:
                await websocket.receive_text()
        except (WebSocketDisconnect, Exception):
            pass
        finally:
            handlers = self.stream_handler.get(topic, [])
            try:
                handlers.remove((send_payload, current_loop))
            except ValueError:
                # try to remove by function identity if tuple identity differs
                for h in list(handlers):
                    if getattr(h[0], "__name__", None) == getattr(send_payload, "__name__", None) and h[1] is current_loop:
                        handlers.remove(h)
                        break
            if not handlers:
                self.stream_handler.pop(topic, None)
            await websocket.close()



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
        self._start_agent_loop()
        self.runtime.start()
