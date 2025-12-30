# mqtt_worker.py
import asyncio
import json
import logging
import time
from typing import Any
from gmqtt import Client as MQTTClient
from multiprocessing import Process, Queue, Event

from core.models import MQTTInput
from core.forms import form_dict_to_input

logger = logging.getLogger("mqtt_consumer_worker")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
logger.addHandler(handler)

class _AsyncMQTTConsumer:
    def __init__(self, cfg: MQTTInput, data_queue: Queue, new_topic_queue: Queue, stop_event: Any):
        self.cfg = cfg
        self.data_queue = data_queue
        self.new_topic_queue = new_topic_queue
        self.stop_event = stop_event

        self.client = None
        self.topics = set()
        self._loop = asyncio.get_event_loop()

    def start_client(self):
        self.client = MQTTClient(self.cfg.client_id, clean_session=self.cfg.clean_session)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect

    async def connect(self):
        
        await self.client.connect(
            self.cfg.broker_ip, 
            self.cfg.broker_port, 
            ssl=self.cfg.ssl, 
            keepalive=self.cfg.keepalive
            )

    def _on_connect(self, client, flags, rc, properties):
        logger.info("MQTT connected, subscribing to %s", self.cfg.topic)
        client.subscribe(self.cfg.topic, qos=self.cfg.qos)

    def _on_disconnect(self, client, packet, exc=None):
        logger.info("MQTT disconnected")

    async def _put_mp_queue(self, mp_queue: Queue, obj: Any, timeout: float = 0.2):
        try:
            await self._loop.run_in_executor(None, mp_queue.put, obj)
        except Exception as e:
            logger.exception("Failed to put to mp queue: %s", e)

    def _on_message(self, client, topic, payload, qos, properties):
        try:
            decoded = payload.decode() if isinstance(payload, (bytes, bytearray)) else str(payload)
        except Exception:
            decoded = str(payload)

        if topic not in self.topics:
            self.topics.add(topic)
            # fire-and-forget
            asyncio.create_task(self._put_mp_queue(self.new_topic_queue, {"topic": topic, "timestamp": time.time()}))

        parsed = None
        try:
            parsed = json.loads(decoded)
        except Exception:
            parsed = None # Deliver no data
            logger.error(f"Unknown data format : {decoded}")

        msg = {
            "topic": topic,
            "payload": parsed,
            "qos": qos,
            "timestamp": time.time(),
        }

        asyncio.create_task(self._put_mp_queue(self.data_queue, msg))

    async def run_forever(self):
        self.start_client()
        backoff = 1
        max_backoff = 10
        while not self.stop_event.is_set():
            try:
                logger.info("Trying to connect to MQTT broker %s:%s", self.cfg.broker_ip, self.cfg.broker_port)
                await self.connect()
                backoff = 1
                
                while not self.stop_event.is_set():
                    await asyncio.sleep(0.5)
                    
                logger.info("Stop event set, disconnecting MQTT client")
                await self.client.disconnect()
                break
            except Exception as e:
                logger.exception("MQTT connect/loop error: %s", e)
                # bekleyip tekrar dene
                await asyncio.sleep(backoff)
                backoff = min(max_backoff, backoff * 2)

        logger.info("AsyncMQTTConsumer exiting run_forever.")


async def _worker_async_main(
        cfg: MQTTInput, 
        data_queue: Queue, 
        new_topic_queue: Queue, 
        stop_event: Any
        ):
    consumer = _AsyncMQTTConsumer(
        cfg, 
        data_queue,
        new_topic_queue, 
        stop_event
        )
    await consumer.run_forever()

def _worker_entry(cfg_dict: dict, data_queue: Queue, new_topic_queue: Queue, stop_event: Any):
    logger.info("Worker process starting with config: %s", cfg_dict)
    cfg = form_dict_to_input(cfg_dict)
    try:
        asyncio.run(_worker_async_main(cfg, data_queue, new_topic_queue, stop_event))
    except KeyboardInterrupt:
        logger.info("Worker process got KeyboardInterrupt")
    except Exception:
        logger.exception("Worker process exception")
    finally:
        logger.info("Worker process exiting.")

