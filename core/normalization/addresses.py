from __future__ import annotations

from functools import lru_cache

from core.geography.madrid_street_catalog import MadridStreetCatalog, parse_address_text
from core.normalization.text import normalize_text, normalize_text_key
from core.runtime_paths import PROCESSED_RESOURCE_DIR


CATALOG_PATH = PROCESSED_RESOURCE_DIR / "madrid_street_catalog.json"


@lru_cache(maxsize=1)
def get_madrid_street_catalog() -> MadridStreetCatalog | None:
    if not CATALOG_PATH.exists():
        return None
    return MadridStreetCatalog.from_file(CATALOG_PATH)


def _format_parsed_address(
    *,
    street_type: str | None,
    street_name: str | None,
    house_number: str | None,
    house_suffix: str | None,
) -> str | None:
    if not street_name:
        return None

    bits = []
    if street_type:
        bits.append(street_type)
    bits.append(street_name)

    text = " ".join(bits).strip()
    if not text:
        return None

    if house_number:
        suffix = house_suffix.lower() if house_suffix else ""
        number = f"{house_number}{suffix}"
        return f"{text}, {number}"

    return text


def extract_address_core(value: str | None) -> str | None:
    parsed = parse_address_text(value)
    if not parsed.clean_text:
        return None

    formatted = _format_parsed_address(
        street_type=parsed.street_type,
        street_name=parsed.street_name,
        house_number=parsed.house_number,
        house_suffix=parsed.house_suffix,
    )
    if formatted:
        return formatted

    return normalize_text(parsed.clean_text)


def normalize_address_raw(value: str | None) -> str | None:
    parsed = parse_address_text(value)
    if not parsed.clean_text:
        return None

    formatted = _format_parsed_address(
        street_type=parsed.street_type,
        street_name=parsed.street_name,
        house_number=parsed.house_number,
        house_suffix=parsed.house_suffix,
    )
    if formatted:
        return normalize_text(formatted)

    return parsed.clean_text


def normalize_address_key(value: str | None) -> str | None:
    parsed = parse_address_text(value)
    if not parsed.lookup_key:
        core = extract_address_core(value)
        return normalize_text_key(core)
    return normalize_text_key(parsed.lookup_key)
