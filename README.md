# Plotune MQTT Extension

The **Plotune MQTT Extension** is a high-performance, modular data connector designed to bridge MQTT-based industrial data streams into the **Plotune** ecosystem. It enables real-time data ingestion, dynamic topic discovery, and bidirectional communication (bridging) between MQTT brokers and the Plotune visual analysis platform.

## Overview

This extension operates as a multi-layered agent that manages concurrent data streams using a hybrid approach of **multiprocessing** (for CPU-bound message parsing) and **asyncio** (for IO-bound communication). It integrates seamlessly with the Plotune SDK to provide a native user interface for MQTT configuration and real-time monitoring.

### Key Capabilities

* **Dynamic Discovery:** Automatically identifies new MQTT topics and registers them as variables within Plotune.
* **Dual-Role Support:** Functions as both a **Consumer** (reading data from brokers) and a **Producer** (bridging/writing Plotune variables back to MQTT).
* **High-Concurrency Engine:** Utilizes isolated worker processes for MQTT client management to ensure the main UI and API remain responsive.
* **Real-time Streaming:** Provides low-latency data updates via WebSockets for instantaneous visualization.
* **Normalization:** Automatically parses and flattens JSON payloads into standardized Plotune signal formats.

---

## Technical Architecture

The extension is built on a robust architecture designed for industrial stability:

1. **MQTTAgent (Main Controller):** Manages the lifecycle of the extension, handles signal variables in memory, and coordinates between worker processes.
2. **Consumer Workers:** Each MQTT connection runs in a dedicated `multiprocessing.Process` to prevent GIL (Global Interpreter Lock) bottlenecks.
3. **Task Bridge:** A specialized async task that monitors specific Plotune variables and publishes changes back to an MQTT broker.
4. **Runtime Push Queue:** A thread-safe queue mechanism that ensures topic registration and metadata updates are handled safely on the main event loop.

---

## Configuration & Installation

### Requirements

* **Python:** 3.9 or higher
* **Platform:** Linux (x86_64) - *Optimized for industrial gateways*
* **Plotune SDK:** Included in dependencies

### Deployment

To build the extension for deployment on a Plotune instance, use the provided build script:

```bash
chmod +x build.linux.sh
./build.linux.sh

```

The script will generate a `dist/plotune-mqtt-extension-linux-x86_64.tar.gz` package containing the binary executable and the `plugin.json` manifest.

---

## Usage

### 1. Connecting to a Broker

Once the extension is active in Plotune, navigate to the extension's configuration form. You can define:

* **Broker Address:** (e.g., `mqtt://broker.hivemq.com`)
* **Broker Port:** (e.g., `1883`)
* **Topics:** List of topics to subscribe to.
* **Producer Mode:** Enable this if you intend to write data back to this broker.

### 2. Signal Mapping

The extension automatically converts MQTT topics into Plotune-compatible keys by replacing slashes (`/`) with underscores (`_`). For example:

* `factory/line1/sensor/temp`  `factory_line1_sensor_temp`

### 3. Bridging Variables

To push a Plotune internal variable to an MQTT topic:

1. Configure a broker as a **Producer**.
2. Trigger the `on_bridge` event via the Plotune UI.
3. The agent will start an asynchronous bridge task to mirror all variable updates to the specified topic.

---

## Internal API Endpoints

The extension exposes several internal endpoints managed by the Plotune Runtime:

| Endpoint | Method | Description |
| --- | --- | --- |
| `/form` | GET | Fetches the dynamic MQTT configuration form. |
| `/form` | POST | Submits broker credentials and initializes new workers. |
| `/bridge/{name}` | POST | Initiates a bridge task for a specific variable. |
| `/unbridge/{name}` | POST | Stops an active bridge task. |
| `/ws` | WS | Real-time websocket stream for signal data. |

---

## Testing

**Running an broker for testing**
```bash
    podman run -d --name mqtt-broker -p 1883:1883 -p 9001:9001 eclipse-mosquitto mosquitto -c /mosquitto-no-auth.conf
    
```
**Consumer/Producer Tests**
```bash
    python consumer.py &
    python producer.py &
```

## Development & Logging

Logs are channeled through the `mqtt_agent` logger. For debugging, ensure your environment is set to `INFO` or `DEBUG` level:

```python
logger.setLevel(logging.INFO)

```

The extension handles `SIGINT` and `SIGTERM` signals gracefully, ensuring all sub-processes and MQTT clients are disconnected and joined before the main process exits.
