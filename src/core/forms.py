from plotune_sdk import FormLayout
from utils.constant_helper import get_config

from .models import MQTTInput


def dynamic_mqtt_form() -> dict:
    """
    Build and return the dynamic relay configuration form schema.
    """
    form = FormLayout()

    # ------------------------------------------------------------------
    # Source
    # ------------------------------------------------------------------
    (
        form.add_tab("Broker")
        .add_text(
            "broker_ip",
            "Broker Host IP",
            default="127.0.0.1",
            required=True
        )
        .add_text(
            "broker_port",
            "Broker Port",
            default="1883",
            required=True
        )
        .add_text(
            "keepalive",
            "Keep Alive",
            default="60",
        )
        .add_checkbox(
            "clean_session",
            "Clean Session",
            default=True
        )
        .add_combobox(
            "qos",
            "QoS",
            ["0 : Fire Forget","1 : At least once","2 : Exactly once"]
        )
        .add_checkbox(
            "ssl",
            "SSL",
            default=False,
        )
    )
    
    (
        form.add_tab("Topic")
        .add_text(
            "topic",
            "Topic",
            default="plotune/#",
        )
        .add_checkbox(
            "producer",
            "Set as producer",
            default=False,
        )
    )
    

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------
    connection = get_config().get("connection", {})
    host = connection.get("ip", "127.0.0.1")
    port = connection.get("port", "")
    base_url = f"http://{host}:{port}"

    (
        form.add_group("Actions")
        .add_button(
            "start",
            "Start",
            {
                "method": "POST",
                "url": f"{base_url}/start",
                "payload_fields": [
                ],
            },
        )
    )

    return form.to_schema()


def form_dict_to_input(data: dict) -> MQTTInput:
    """
    Convert submitted form data into a MQTTInput model.
    """

    def safe_int(value, default: int | None = None) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    return MQTTInput(
        topic= data.get("topic"),
        broker_ip=  data.get("broker_ip"),
        broker_port=  safe_int(data.get("broker_port")) ,
        producer= bool(data.get("producer")),
        qos=  safe_int(data.get("qos")[0]),
        keepalive=  safe_int(data.get("keepalive")),
        clean_session= bool(data.get("clean_session")),
        ssl = bool(data.get("ssl", False))
    )
