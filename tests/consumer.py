import asyncio
import uuid
import logging
from gmqtt import Client as MQTTClient

BROKER_HOST = "127.0.0.1"
BROKER_PORT = 1883
SUBSCRIBE_TOPIC = "plotune/#"  # '#' is the wild card for topics
# 0: Fire and forget , 1: At least once. Guaranteed delivery, 2: Exactly once.
QOS = 1

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DISCOVERED_TOPICS = set()


def on_connect(client, flags, rc, properties):
    logger.info("Connected successfully")
    client.subscribe(SUBSCRIBE_TOPIC, qos=QOS)


def on_message(client, topic, payload, qos, properties):
    if topic not in DISCOVERED_TOPICS:
        DISCOVERED_TOPICS.add(topic)
        logger.warning(f"NEW TOPIC DISCOVERED: {topic}")

    decoded_payload = payload.decode()
    logger.info(f"RECV [{topic}] -> {decoded_payload}")


def on_disconnect(client, packet, exc=None):
    logger.info("Disconnected")


async def main():
    client = MQTTClient(f"consumer-{uuid.uuid4()}")
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    try:
        await client.connect(BROKER_HOST, BROKER_PORT)
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        return

    try:
        await asyncio.Future()
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Stopping consumer")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
