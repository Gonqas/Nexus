from __future__ import annotations

from pathlib import Path

import orjson


BASE_DIR = Path(__file__).resolve().parents[2]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
FULL_CATALOG_PATH = PROCESSED_DIR / "madrid_external_context_catalog_full.json"
FOCUS_CATALOG_PATH = PROCESSED_DIR / "madrid_external_context_catalog_focus.json"
SUMMARY_PATH = PROCESSED_DIR / "madrid_external_context_catalog_summary.json"


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return orjson.loads(path.read_bytes())


def load_external_context_summary() -> dict:
    return _read_json(SUMMARY_PATH)


def load_external_context_focus_catalog(
    *,
    theme: str | None = None,
    portal_id: str | None = None,
    limit: int | None = None,
) -> dict:
    payload = _read_json(FOCUS_CATALOG_PATH)
    datasets = list(payload.get("datasets") or [])

    if theme:
        datasets = [dataset for dataset in datasets if dataset.get("primary_theme") == theme]
    if portal_id:
        datasets = [dataset for dataset in datasets if dataset.get("portal_id") == portal_id]
    if limit is not None:
        datasets = datasets[:limit]

    return payload | {"datasets": datasets, "dataset_count": len(datasets)}
