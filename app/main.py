from __future__ import annotations

import sys
import traceback
from pathlib import Path

from PySide6.QtWidgets import QApplication


if __package__ in (None, ""):
    ROOT_DIR = Path(__file__).resolve().parents[1]
    if str(ROOT_DIR) not in sys.path:
        sys.path.insert(0, str(ROOT_DIR))

    from core.runtime_paths import LOG_DATA_DIR
    from db.init_db import init_database
    from app.ui.main_window import MainWindow
    from app.ui.theme import build_app_stylesheet
else:
    from core.runtime_paths import LOG_DATA_DIR
    from db.init_db import init_database
    from .ui.main_window import MainWindow
    from .ui.theme import build_app_stylesheet


LOG_DIR = LOG_DATA_DIR
LOG_DIR.mkdir(parents=True, exist_ok=True)
STARTUP_LOG_PATH = LOG_DIR / "startup.log"


def log_startup_exception(exc: Exception) -> None:
    trace = "".join(traceback.format_exception(exc))
    STARTUP_LOG_PATH.write_text(trace, encoding="utf-8")


def main() -> int:
    try:
        init_database()

        app = QApplication(sys.argv)
        app.setApplicationName("Nexus Madrid")
        app.setStyleSheet(build_app_stylesheet())
        window = MainWindow()
        window.show()
        return app.exec()
    except Exception as exc:
        log_startup_exception(exc)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
