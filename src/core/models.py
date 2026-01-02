from uuid import uuid4
from dataclasses import dataclass, field
from collections import deque

MAX_HISTORY = 240


@dataclass
class MQTTInput:
    topic: str
    producer: bool
    broker_ip: str
    broker_port: int = 1883
    qos: int = 1
    keepalive: int = 60
    clean_session: bool = True
    ssl: bool = False
    client_id: str = field(default_factory=lambda: f"plotune-{uuid4().hex[:6]}")


@dataclass
class SignalVariable:
    name: str
    payload: deque = None

    def __post_init__(self):
        self.payload: deque = deque(maxlen=MAX_HISTORY)
