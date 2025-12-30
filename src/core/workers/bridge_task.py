# bridge_task.py
import json
import asyncio
import aiohttp
import logging
import time
import uuid
from typing import Dict, Tuple, Any, Optional
from gmqtt import Client as MQTTClient

from plotune_sdk.models import Variable
from core.models import MQTTInput

logger = logging.getLogger("bridge")
logger.setLevel(logging.INFO)

# client cache keyed by (host, port, client_id)
_clients: Dict[Tuple[str, int, str], Dict[str, Any]] = {}
_clients_lock = asyncio.Lock()


async def _ensure_client_connected(conf: MQTTInput, timeout: float = 5.0) -> MQTTClient:
    key = (conf.broker_ip, conf.broker_port, conf.client_id)
    async with _clients_lock:
        entry = _clients.get(key)
        if entry and entry.get("connected"):
            return entry["client"]

        # create client and placeholders
        client = MQTTClient(conf.client_id or f"producer-{uuid.uuid4().hex[:8]}")
        state = {"client": client, "connected": False, "lock": asyncio.Lock()}

        def _on_connect(c, flags, rc, properties):
            logger.info("MQTT producer connected to %s:%s", conf.broker_ip, conf.broker_port)
            state["connected"] = True

        def _on_disconnect(c, packet, exc=None):
            logger.warning("MQTT producer disconnected from %s:%s", conf.broker_ip, conf.broker_port)
            state["connected"] = False

        client.on_connect = _on_connect
        client.on_disconnect = _on_disconnect

        _clients[key] = state

    # connect under per-client lock to avoid concurrent connect attempts
    async with state["lock"]:
        if state["connected"]:
            return state["client"]
        try:
            await client.connect(conf.broker_ip, conf.broker_port, ssl=conf.ssl, keepalive=conf.keepalive)
            # give gmqtt a moment to mark connected via callback
            await asyncio.sleep(0.05)
            state["connected"] = True
            return state["client"]
        except Exception:
            state["connected"] = False
            raise


def _normalize_topic_base(topic: str) -> str:
    if topic.endswith("/"):
        return topic[:-1]
    return topic


def _normalize_payload(raw: Any) -> Dict[str, Any]:
    ts = time.time()
    if isinstance(raw, dict):
        value = raw.get("value", raw.get("val", raw))
        when = raw.get("timestamp", raw.get("time", ts))
        try:
            # try to coerce numeric timestamp
            when = float(when)
        except Exception:
            # fallback to unix ts
            when = ts
        return {"value": value, "timestamp": when}
    else:
        return {"value": raw, "timestamp": ts}


async def produce_data(conf: MQTTInput, payload: dict, variable: Variable):
    client = await _ensure_client_connected(conf)
    topic_base = _normalize_topic_base(conf.topic)
    target_topic = f"{topic_base}/{variable.name}"

    norm = _normalize_payload(payload)
    body = json.dumps(norm)
    try:
        # gmqtt.publish is synchronous-ish, safe to call from asyncio context
        client.publish(target_topic, body, qos=conf.qos)
    except Exception:
        logger.exception("Failed to publish to %s", target_topic)
        # Mark client disconnected so next publish will reconnect
        # find state and mark disconnected
        key = (conf.broker_ip, conf.broker_port, conf.client_id)
        state = _clients.get(key)
        if state:
            state["connected"] = False
        raise


async def fetch_data(url: str, conf: MQTTInput, variable: Variable, stop_event) -> None:
    backoff = 1.0
    max_backoff = 10.0
    session_timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=None)

    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        while True:
            if callable(getattr(stop_event, "is_set", None)) and stop_event.is_set():
                return

            try:
                async with session.ws_connect(url) as ws:
                    backoff = 1.0
                    async for msg in ws:
                        if callable(getattr(stop_event, "is_set", None)) and stop_event.is_set():
                            await ws.close()
                            return

                        try:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    raw = json.loads(msg.data)
                                except Exception:
                                    raw = msg.data
                                await produce_data(conf, raw, variable)

                            elif msg.type == aiohttp.WSMsgType.BINARY:
                                await produce_data(conf, {"value": msg.data}, variable)

                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                logger.error("[%s] WS error: %s", variable.name, ws.exception())
                                break

                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            logger.exception("[%s] Unexpected error while handling ws message", variable.name)
                    # if we dropped out of async for, try reconnect
            except aiohttp.ClientConnectorError as e:
                logger.warning("[%s] WS connect failed: %s", variable.name, e)
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("[%s] WS loop unexpected error", variable.name)

            # backoff and check stop_event
            await asyncio.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)


async def task_bridge_from_ws_to_mqtt(variable: Variable, conf: MQTTInput, stop_event) -> None:
    schema = "wss" if conf.ssl else "ws"
    base = _normalize_topic_base(conf.topic)
    # variable.name is expected already normalized (you mentioned using '_' hack). Do not modify here.
    url = f"{schema}://{variable.source_ip}:{variable.source_port}/fetch/{variable.name}"
    await fetch_data(url, conf, variable, stop_event)
