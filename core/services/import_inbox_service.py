from __future__ import annotations

from pathlib import Path

from core.runtime_paths import INBOX_DATA_DIR


BASELINE_INBOX_DIR = INBOX_DATA_DIR / "baseline_uploads"
ALLOWED_SUFFIXES = {".csv", ".xlsx", ".xls"}


def ensure_baseline_inbox_dir() -> Path:
    BASELINE_INBOX_DIR.mkdir(parents=True, exist_ok=True)
    return BASELINE_INBOX_DIR


def is_supported_baseline_file(path: str | Path) -> bool:
    file_path = Path(path)
    return file_path.is_file() and file_path.suffix.lower() in ALLOWED_SUFFIXES


def list_pending_baseline_files() -> list[str]:
    inbox_dir = ensure_baseline_inbox_dir()
    paths = [
        path
        for path in inbox_dir.iterdir()
        if is_supported_baseline_file(path)
    ]
    paths.sort(key=lambda path: (path.stat().st_mtime, path.name), reverse=True)
    return [str(path) for path in paths]
