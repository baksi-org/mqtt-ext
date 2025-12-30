import asyncio
import uuid
import random
import json
import logging
import time
from gmqtt import Client as MQTTClient

BROKER_HOST = "127.0.0.1"
BROKER_PORT = 1883
TOPICS = [
    "plotune/sensor/temperature",
    "plotune/sensor/pressure",
    "plotune/machine/motor_speed",
    "plotune/factory/status",
    "plotune/energy/consumption"
]
MIN_INTERVAL = 0.1
MAX_INTERVAL = 0.3
QOS = 1

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def producer_loop(client):
    while True:
        try:
            target_topic = random.choice(TOPICS)
            payload = {
                "value": round(random.uniform(20.0, 100.0), 2),
                "timestamp": time.time(),
                "client_id": client._client_id
            }
            client.publish(target_topic, json.dumps(payload), qos=QOS)
            logger.info(f"Published to {target_topic}")
            await asyncio.sleep(random.uniform(MIN_INTERVAL, MAX_INTERVAL))
        except Exception as e:
            logger.error(f"Error: {e}")
            break

async def main():
    client = MQTTClient(f"producer-{uuid.uuid4()}")
    
    try:
        await client.connect(BROKER_HOST, BROKER_PORT)
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        return

    task = asyncio.create_task(producer_loop(client))
    try:
        await asyncio.Future()
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Stopping producer")
    finally:
        task.cancel()
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())