from __future__ import annotations

from pathlib import Path

import orjson

from core.features.location_labels import canonical_zone_label
from core.normalization.text import normalize_text_key
from core.runtime_paths import PROCESSED_RESOURCE_DIR


PROCESSED_DIR = PROCESSED_RESOURCE_DIR
ZONE_CONTEXT_PATH = PROCESSED_DIR / "madrid_zone_external_context.json"

_CACHE_SIGNATURE: tuple[int, int] | None = None
_CACHE_PAYLOAD: dict | None = None


def _path_signature(path: Path) -> tuple[int, int] | None:
    if not path.exists():
        return None
    stat = path.stat()
    return (int(stat.st_mtime_ns), int(stat.st_size))


def _normalize_zone_label(value: str | None) -> str | None:
    canonical = canonical_zone_label(value)
    return normalize_text_key(canonical)


def load_external_zone_context() -> dict:
    global _CACHE_SIGNATURE, _CACHE_PAYLOAD

    signature = _path_signature(ZONE_CONTEXT_PATH)
    if signature is None:
        return {}
    if _CACHE_SIGNATURE == signature and _CACHE_PAYLOAD is not None:
        return _CACHE_PAYLOAD

    payload = orjson.loads(ZONE_CONTEXT_PATH.read_bytes())
    _CACHE_SIGNATURE = signature
    _CACHE_PAYLOAD = payload
    return payload


def get_zone_external_context(zone_label: str | None) -> dict:
    payload = load_external_zone_context()
    if not payload:
        return {}

    zone_key = _normalize_zone_label(zone_label)
    if not zone_key:
        return {}

    neighborhoods = payload.get("neighborhoods") or {}
    districts = payload.get("districts") or {}
    return dict(neighborhoods.get(zone_key) or districts.get(zone_key) or {})
