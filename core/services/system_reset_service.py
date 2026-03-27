from __future__ import annotations

from datetime import datetime
import shutil
from pathlib import Path

from db.base import Base
from db.session import DB_PATH, engine
import db.models  # noqa: F401
from core.runtime_paths import APP_DATA_DIR


BACKUP_DIR = APP_DATA_DIR / "backups"


def _sidecar_paths(db_path: Path) -> list[Path]:
    return [
        db_path.with_name(db_path.name + "-wal"),
        db_path.with_name(db_path.name + "-shm"),
    ]


def reset_runtime_database(*, create_backup: bool = True) -> dict[str, str | None]:
    APP_DATA_DIR.mkdir(parents=True, exist_ok=True)
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    backup_path: Path | None = None
    engine.dispose()

    if DB_PATH.exists() and DB_PATH.stat().st_size > 0 and create_backup:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = BACKUP_DIR / f"nexus_madrid_{timestamp}.sqlite"
        shutil.copy2(DB_PATH, backup_path)

    reset_mode = "file"
    try:
        if DB_PATH.exists():
            DB_PATH.unlink()

        for sidecar in _sidecar_paths(DB_PATH):
            if sidecar.exists():
                sidecar.unlink()

        Base.metadata.create_all(bind=engine)
    except PermissionError:
        reset_mode = "schema"
        Base.metadata.drop_all(bind=engine)
        Base.metadata.create_all(bind=engine)

    return {
        "db_path": str(DB_PATH),
        "backup_path": str(backup_path) if backup_path else None,
        "reset_mode": reset_mode,
    }
