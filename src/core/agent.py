from plotune_sdk import PlotuneRuntime
from plotune_sdk.models import FileMetaData, FileReadRequest
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
import os
import asyncio
import logging
import signal
import time
from threading import Thread
from typing import Any, Dict, List, Optional
from asyncio import Task
from multiprocessing import Process, Queue, Event
from core.models import MQTTInput
from core.workers import consumer_worker_entry
from utils import get_config, get_custom_config
from .forms import dynamic_mqtt_form, form_dict_to_input
from dataclasses import asdict

logger = logging.getLogger(__name__)

class MQTTAgent:
    def __init__(self):
        self.config = get_config()
        self.custom_config = get_custom_config()

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
            topic = msg.get("topic")
            _timestamp = msg.get("timestamp", "No information")
            if topic:
                try:
                    await self.runtime.core_client.add_variable(
                        variable_name=topic,
                        variable_desc=f"{_timestamp}",
                    )
                except Exception:
                    logger.exception("Failed to add variable for topic %s", topic)

    async def handle_data_queue(self, data_q: Queue):
        while not self._stop_event.is_set():
            msg = await self._mp_queue_get(data_q, timeout=1.0)
            if not msg:
                continue
            try:
                topic = msg.get("topic")
                payload = msg.get("payload")
                qos = msg.get("qos")
                timestamp = msg.get("timestamp")
                # processing logic here
            except Exception:
                logger.exception("Error processing message from data queue")

    async def stream(
        self,
        signal_name: str,
        websocket: WebSocket,
        data: Any,
    ) -> None:
        logger.info("Client requested signal '%s'", signal_name)

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
