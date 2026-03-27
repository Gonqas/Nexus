from pathlib import Path

from sqlalchemy import create_engine

import core.services.system_reset_service as system_reset_service
from db.base import Base
import db.models  # noqa: F401


def test_reset_runtime_database_recreates_empty_db_and_backup(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "nexus_madrid.sqlite"
    backup_dir = tmp_path / "backups"
    app_data_dir = tmp_path / "appdata"
    engine = create_engine(f"sqlite:///{db_path}", future=True)

    Base.metadata.create_all(bind=engine)
    db_path.write_text("seed", encoding="utf-8")

    monkeypatch.setattr(system_reset_service, "DB_PATH", db_path)
    monkeypatch.setattr(system_reset_service, "BACKUP_DIR", backup_dir)
    monkeypatch.setattr(system_reset_service, "APP_DATA_DIR", app_data_dir)
    monkeypatch.setattr(system_reset_service, "engine", engine)

    result = system_reset_service.reset_runtime_database(create_backup=True)

    assert db_path.exists() is True
    assert Path(str(result["backup_path"])).exists() is True
