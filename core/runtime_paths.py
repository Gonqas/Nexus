from __future__ import annotations

import os
import sys
from pathlib import Path


APP_NAME = "NexusMadrid"
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _bundle_root() -> Path:
    if getattr(sys, "frozen", False):
        meipass = getattr(sys, "_MEIPASS", None)
        if meipass:
            return Path(meipass)
        return Path(sys.executable).resolve().parent
    return PROJECT_ROOT


def _user_data_root() -> Path:
    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data) / APP_NAME
    return Path.home() / f".{APP_NAME.lower()}"


BUNDLE_ROOT = _bundle_root()
RESOURCE_DATA_DIR = BUNDLE_ROOT / "data"
PROCESSED_RESOURCE_DIR = RESOURCE_DATA_DIR / "processed"

APP_DATA_DIR = _user_data_root()
RAW_DATA_DIR = APP_DATA_DIR / "raw"
STATE_DATA_DIR = APP_DATA_DIR / "state"
INBOX_DATA_DIR = APP_DATA_DIR / "inbox"
LOG_DATA_DIR = APP_DATA_DIR / "logs"
DEBUG_DATA_DIR = APP_DATA_DIR / "debug" / "casafari"

for path in (
    APP_DATA_DIR,
    RAW_DATA_DIR,
    STATE_DATA_DIR,
    INBOX_DATA_DIR,
    LOG_DATA_DIR,
    DEBUG_DATA_DIR,
):
    path.mkdir(parents=True, exist_ok=True)
