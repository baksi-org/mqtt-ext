import json
import os
import sys
from functools import lru_cache
from typing import Dict, Any

USE_AVAILABLE_PORT = os.environ.get("USE_AVAILABLE_PORT", "true").lower() == "true"
SERVER_PORT = int(os.environ.get("SERVER_PORT", 9000))


if getattr(sys, "frozen", False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.dirname(__file__))

CONFIG_PATH = os.path.join(BASE_DIR, "plugin.json")

if USE_AVAILABLE_PORT:
    from plotune_sdk.utils import AVAILABLE_PORT


@lru_cache(maxsize=1)
def get_config() -> Dict[str, Any]:
    """
    Load and cache plugin configuration.

    If USE_AVAILABLE_PORT is enabled, the runtime will bind
    to an automatically selected available port.
    """
    with open(CONFIG_PATH, "r", encoding="utf-8-sig") as file:
        config = json.load(file)

    if USE_AVAILABLE_PORT:
        config["connection"]["port"] = AVAILABLE_PORT
    else:
        config["connection"]["port"] = SERVER_PORT

    return config


@lru_cache(maxsize=1)
def get_custom_config() -> Dict[str, Any]:
    """
    Return the custom configuration section from plugin config.
    """
    config = get_config()
    return config.get("configuration", {})
