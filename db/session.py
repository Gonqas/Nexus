from __future__ import annotations

import shutil

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.runtime_paths import APP_DATA_DIR, RESOURCE_DATA_DIR


DB_NAME = "nexus_madrid.sqlite"
DB_PATH = APP_DATA_DIR / DB_NAME
RESOURCE_DB_PATH = RESOURCE_DATA_DIR / DB_NAME
DATABASE_URL = f"sqlite:///{DB_PATH}"


def _bootstrap_database_file() -> None:
    APP_DATA_DIR.mkdir(parents=True, exist_ok=True)

    current_exists = DB_PATH.exists()
    current_size = DB_PATH.stat().st_size if current_exists else 0
    bundled_exists = RESOURCE_DB_PATH.exists()
    bundled_size = RESOURCE_DB_PATH.stat().st_size if bundled_exists else 0

    should_seed_from_bundle = bundled_exists and bundled_size > 0 and (
        not current_exists or current_size == 0
    )
    if should_seed_from_bundle:
        shutil.copy2(RESOURCE_DB_PATH, DB_PATH)
        return

    if not current_exists:
        DB_PATH.touch()


_bootstrap_database_file()

engine = create_engine(
    DATABASE_URL,
    echo=False,
    future=True,
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    future=True,
)
