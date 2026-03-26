from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from core.geography.madrid_street_catalog import MadridStreetCatalog, parse_address_text
from core.normalization.text import normalize_text, normalize_text_key


BASE_DIR = Path(__file__).resolve().parents[2]
CATALOG_PATH = BASE_DIR / "data" / "processed" / "madrid_street_catalog.json"


@lru_cache(maxsize=1)
def get_madrid_street_catalog() -> MadridStreetCatalog | None:
    if not CATALOG_PATH.exists():
        return None
    return MadridStreetCatalog.from_file(CATALOG_PATH)


def extract_address_core(value: str | None) -> str | None:
    parsed = parse_address_text(value)
    if not parsed.clean_text:
        return None

    if parsed.street_name and parsed.house_number:
        if parsed.street_type:
            return f"{parsed.street_type} {parsed.street_name}, {parsed.house_number}"
        return f"{parsed.street_name}, {parsed.house_number}"

    if parsed.street_name:
        if parsed.street_type:
            return f"{parsed.street_type} {parsed.street_name}"
        return parsed.street_name

    return normalize_text(value)


def normalize_address_raw(value: str | None) -> str | None:
    parsed = parse_address_text(value)
    return parsed.clean_text


def normalize_address_key(value: str | None) -> str | None:
    parsed = parse_address_text(value)
    if not parsed.lookup_key:
        core = extract_address_core(value)
        return normalize_text_key(core)
    return normalize_text_key(parsed.lookup_key)