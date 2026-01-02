import json
from pathlib import Path

p = Path("dist/plugin.json")

data = json.loads(p.read_text(encoding="utf-8"))

data["cmd"] = ["plotune_mqtt_extension.exe"]

p.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
