import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright
from core.services.casafari_semantics_service import infer_event_type_from_context_urls
from core.config.settings import (
    CASAFARI_BOOTSTRAP_DAYS,
    CASAFARI_DEBUG_BASE_DIR,
    CASAFARI_HEADLESS,
    CASAFARI_HISTORY_BASE_URL,
    CASAFARI_MAX_PAGES_PER_SYNC,
    CASAFARI_PAGE_CHANGE_MAX_WAIT_MS,
    CASAFARI_PROFILE_DIR,
    CASAFARI_STORAGE_STATE_PATH,
    CASAFARI_SYNC_OVERLAP_HOURS,
    CASAFARI_VERIFIED_HISTORY_URL_PATH,
    CASAFARI_WAIT_AFTER_GOTO_MS,
    CASAFARI_WAIT_AFTER_PAGINATION_MS,
    CASAFARI_WAIT_BETWEEN_PAGE_CHECKS_MS,
)
from core.normalization.portals import (
    canonicalize_portal_label,
    normalize_portal_key as normalize_portal_key_base,
)
from core.services.casafari_session_service import looks_like_history_body
from core.normalization.text import normalize_text, normalize_text_key
from core.normalization.urls import normalize_url


MONTHS_ES = {
    "ene": 1,
    "feb": 2,
    "mar": 3,
    "abr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dic": 12,
}

JSON_URL_HINTS = (
    "history",
    "alert",
    "search",
    "saved-search",
    "listing",
    "property",
    "estate",
    "result",
    "graphql",
)

TITLE_KEYS = ("title", "name", "headline", "description", "label")
URL_KEYS = ("url", "link", "href", "listingUrl", "propertyUrl", "detailUrl", "externalUrl")
ADDRESS_KEYS = ("address", "street", "streetAddress", "fullAddress", "location", "locationLabel")
PORTAL_KEYS = ("portal", "source", "sourceName", "website", "provider")
PHONE_KEYS = ("phone", "phoneNumber", "mobile", "telephone")
CONTACT_NAME_KEYS = ("contactName", "contact_name", "sellerName", "brokerName", "agentName")
CURRENT_PRICE_KEYS = (
    "currentPrice",
    "current_price",
    "price",
    "salePrice",
    "amount",
    "priceEur",
    "currentPriceEur",
    "sale_price",
)
PREVIOUS_PRICE_KEYS = (
    "previousPrice",
    "previous_price",
    "oldPrice",
    "old_price",
    "originalPrice",
    "previousPriceEur",
)
EVENT_TYPE_KEYS = ("eventType", "historyType", "type", "status")
DATETIME_KEYS = (
    "eventDate",
    "eventDatetime",
    "createdAt",
    "updatedAt",
    "date",
    "datetime",
    "timestamp",
)
SOURCE_EVENT_ID_KEYS = ("eventId", "historyId", "history_id", "activityId", "activity_id")
SOURCE_LISTING_ID_KEYS = ("listingId", "listing_id", "propertyId", "property_id", "estateId", "estate_id")
TOTAL_KEYS = ("total", "totalCount", "total_count", "count", "resultsCount", "results_count")

SUSPICIOUS_TEXT_SNIPPETS = (
    "condition_match",
    "matched condition set",
    "be_layers_allow_poi",
    "be_layers",
    "allow_poi",
    "geometry",
    "coordinates",
    "longitude",
    "latitude",
    "mapbox",
    "tile",
    "layer",
    "polygon",
    "bbox",
)

EVENT_HINTS = (
    "nuevo",
    "bajada de precio",
    "subida de precio",
    "reservado",
    "no disponible",
    "vendido",
    "vencido",
    "new",
    "price drop",
    "price raise",
    "reserved",
    "sold",
    "expired",
)

GENERIC_NAME_TOKENS = {
    "active",
    "none",
    "null",
    "undefined",
    "particular",
    "anuncio particular",
    "profesional",
    "agencia",
    "inmobiliaria",
}

NETWORK_NOISE_HINTS = (
    "what would you like to do first",
    "starting-page",
    "userguiding",
    "faq___",
    "modal",
    "center de ayuda",
)


def utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def to_iso_z(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def load_history_base_url() -> str:
    if CASAFARI_VERIFIED_HISTORY_URL_PATH.exists():
        verified = CASAFARI_VERIFIED_HISTORY_URL_PATH.read_text(encoding="utf-8").strip()
        if verified:
            return verified
    return CASAFARI_HISTORY_BASE_URL


def dedupe_query_values(query: dict[str, list[str]]) -> dict[str, list[str]]:
    cleaned: dict[str, list[str]] = {}
    for key, values in query.items():
        deduped = []
        seen = set()
        for value in values:
            if value is None:
                continue
            value_str = str(value).strip()
            if not value_str:
                continue
            if value_str in seen:
                continue
            seen.add(value_str)
            deduped.append(value_str)
        if deduped:
            cleaned[key] = deduped
    return cleaned


def build_history_url(base_url: str, from_dt: datetime, to_dt: datetime) -> str:
    parsed = urlparse(base_url)
    query = parse_qs(parsed.query)

    query["from"] = [to_iso_z(from_dt)]
    query["to"] = [to_iso_z(to_dt)]
    query["direct"] = ["true"]

    query = dedupe_query_values(query)
    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def derive_sync_range(last_success_to: datetime | None) -> tuple[datetime, datetime]:
    now_dt = utc_now_naive()

    if last_success_to is None:
        return now_dt - timedelta(days=CASAFARI_BOOTSTRAP_DAYS), now_dt

    return last_success_to - timedelta(hours=CASAFARI_SYNC_OVERLAP_HOURS), now_dt


@dataclass(frozen=True)
class CasafariFetchOptions:
    mode: str
    headless: bool
    max_pages: int
    wait_after_goto_ms: int
    wait_after_pagination_ms: int
    wait_between_page_checks_ms: int
    page_change_max_wait_ms: int
    settle_wait_ms: int
    scroll_wait_ms: int


def resolve_fetch_options(sync_mode: str | None) -> CasafariFetchOptions:
    mode = (sync_mode or "balanced").strip().lower()

    if mode == "fast":
        return CasafariFetchOptions(
            mode="fast",
            headless=True,
            max_pages=min(CASAFARI_MAX_PAGES_PER_SYNC, 20),
            wait_after_goto_ms=max(1200, CASAFARI_WAIT_AFTER_GOTO_MS // 2),
            wait_after_pagination_ms=max(900, CASAFARI_WAIT_AFTER_PAGINATION_MS // 2),
            wait_between_page_checks_ms=max(250, CASAFARI_WAIT_BETWEEN_PAGE_CHECKS_MS // 2),
            page_change_max_wait_ms=max(4500, CASAFARI_PAGE_CHANGE_MAX_WAIT_MS // 2),
            settle_wait_ms=900,
            scroll_wait_ms=300,
        )

    if mode == "diagnostic":
        return CasafariFetchOptions(
            mode="diagnostic",
            headless=False,
            max_pages=CASAFARI_MAX_PAGES_PER_SYNC,
            wait_after_goto_ms=max(5000, CASAFARI_WAIT_AFTER_GOTO_MS),
            wait_after_pagination_ms=max(2800, CASAFARI_WAIT_AFTER_PAGINATION_MS),
            wait_between_page_checks_ms=max(700, CASAFARI_WAIT_BETWEEN_PAGE_CHECKS_MS),
            page_change_max_wait_ms=max(12000, CASAFARI_PAGE_CHANGE_MAX_WAIT_MS),
            settle_wait_ms=1800,
            scroll_wait_ms=650,
        )

    return CasafariFetchOptions(
        mode="balanced",
        headless=CASAFARI_HEADLESS,
        max_pages=CASAFARI_MAX_PAGES_PER_SYNC,
        wait_after_goto_ms=CASAFARI_WAIT_AFTER_GOTO_MS,
        wait_after_pagination_ms=CASAFARI_WAIT_AFTER_PAGINATION_MS,
        wait_between_page_checks_ms=CASAFARI_WAIT_BETWEEN_PAGE_CHECKS_MS,
        page_change_max_wait_ms=CASAFARI_PAGE_CHANGE_MAX_WAIT_MS,
        settle_wait_ms=1500,
        scroll_wait_ms=600,
    )


def parse_price_value(value: Any) -> float | None:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        numeric = float(value)
        # Para residencial Madrid, valores por debajo de 30k son ruido casi seguro
        if numeric < 30000:
            return None
        if numeric > 50000000:
            return None
        return numeric

    text = normalize_text(str(value))
    if not text:
        return None

    digits = re.sub(r"[^\d]", "", text)
    if not digits:
        return None

    numeric = float(digits)
    if numeric < 30000:
        return None
    if numeric > 50000000:
        return None

    return numeric


def parse_price_values(text: str) -> tuple[float | None, float | None]:
    matches = re.findall(r"€\s*([\d\.,]+)", text)
    values: list[float] = []

    for value in matches:
        parsed = parse_price_value(value)
        if parsed is not None:
            values.append(parsed)

    if not values:
        return None, None

    current_price = values[0]
    previous_price = values[1] if len(values) > 1 else None
    return current_price, previous_price


def parse_price_from_fallback_text(text: str) -> tuple[float | None, float | None]:
    if not text:
        return None, None

    # 1) Primero intenta precios con símbolo €
    current_price, previous_price = parse_price_values(text)
    if current_price is not None:
        return current_price, previous_price

    text_norm = normalize_text(text)
    if not text_norm:
        return None, None

    # 2) Busca números grandes con contexto de precio
    contextual_patterns = [
        r"(?:precio|price|venta|sale)\D{0,20}(\d{5,8})",
        r"(\d{5,8})\D{0,10}(?:eur|€)",
    ]

    contextual_values: list[float] = []
    for pattern in contextual_patterns:
        for match in re.findall(pattern, text_norm, flags=re.IGNORECASE):
            try:
                value = float(match)
            except Exception:
                continue

            if 30000 <= value <= 50000000:
                contextual_values.append(value)

    if contextual_values:
        unique_values = []
        seen = set()
        for value in contextual_values:
            if value in seen:
                continue
            seen.add(value)
            unique_values.append(value)

        current = unique_values[0]
        previous = unique_values[1] if len(unique_values) > 1 else None
        return current, previous

    # 3) Último fallback: solo números de 6 a 8 dígitos
    # Evitamos 10161, 28001, 12345, etc.
    candidates = re.findall(r"\b(\d{6,8})\b", text_norm)
    parsed: list[float] = []

    for raw in candidates:
        try:
            value = float(raw)
        except Exception:
            continue

        if value < 30000:
            continue
        if value > 50000000:
            continue

        parsed.append(value)

    unique_values: list[float] = []
    seen = set()
    for value in parsed:
        if value in seen:
            continue
        seen.add(value)
        unique_values.append(value)

    if not unique_values:
        return None, None

    current = unique_values[0]
    previous = unique_values[1] if len(unique_values) > 1 else None
    return current, previous


def parse_date_from_card(text: str) -> datetime | None:
    compact = " ".join(text.split()).lower()
    match = re.search(
        r"(\d{1,2})\s+(ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)\s+(\d{4})",
        compact,
    )
    if not match:
        return None

    day = int(match.group(1))
    month = MONTHS_ES[match.group(2)]
    year = int(match.group(3))
    return datetime(year, month, day, 12, 0, 0)


def parse_datetime_value(value: Any) -> datetime | None:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        try:
            timestamp = float(value)
            if timestamp > 10_000_000_000:
                timestamp = timestamp / 1000.0
            return datetime.fromtimestamp(timestamp, tz=timezone.utc).replace(tzinfo=None)
        except Exception:
            return None

    text = normalize_text(str(value))
    if not text:
        return None

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(
            timezone.utc
        ).replace(tzinfo=None)
    except Exception:
        return parse_date_from_card(text)


def normalize_phone_key(value: str | None) -> str | None:
    text = normalize_text(value)
    if not text:
        return None

    digits = re.sub(r"\D", "", text)
    if not digits:
        return None

    if digits.startswith("34") and len(digits) >= 11:
        digits = digits[-9:]

    if len(digits) < 9:
        return None

    return digits


def normalize_name_key(value: str | None) -> str | None:
    text = normalize_text_key(value)
    if not text:
        return None
    return text


def normalize_portal_key(value: str | None) -> str | None:
    return normalize_portal_key_base(value)


def build_address_fragment_key(value: str | None) -> str | None:
    text = normalize_text_key(value)
    if not text:
        return None

    text = re.sub(r"\b\d+[a-z]?\b", " ", text)
    text = re.sub(r"\b(calle|cl|avenida|av|paseo|ps|plaza|pl|carretera|camino|via)\b", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return None

    tokens = text.split()
    if not tokens:
        return None

    return " ".join(tokens[:4])


def price_key(value: float | None) -> str:
    if value is None:
        return "na"
    return str(int(round(value)))


def event_date_key(value: datetime | None) -> str:
    if value is None:
        return "na"
    return value.strftime("%Y-%m-%dT%H:%M")


def infer_event_type(title: str | None, fallback_text: str | None = None, raw_type: str | None = None) -> str:
    text = " ".join(
        part for part in (title or "", fallback_text or "", raw_type or "") if part
    ).lower()

    if "bajada de precio" in text:
        return "price_drop"
    if "subida de precio" in text:
        return "price_raise"
    if "reservado" in text:
        return "reserved"
    if "no disponible" in text:
        return "not_available"
    if "vendido" in text:
        return "sold"
    if "vencido" in text:
        return "expired"
    if "nuevo" in text:
        return "listing_detected"

    return "history_item"


def extract_address_from_title(title: str | None) -> str | None:
    title = normalize_text(title)
    if not title:
        return None

    match = re.search(r"en venta en (.+?)(?:\s+-\s+(Barrio|Zona)|$)", title, flags=re.IGNORECASE)
    if match:
        return normalize_text(match.group(1))

    return None


def extract_portal_and_contact(text: str) -> tuple[str | None, str | None]:
    pattern = r"(Idealista|Fotocasa|Habitaclia|Milanuncios|Pisos\.com|Yaencontre)\s*:?\s*([^\n\+€]+)?"
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if not match:
        return None, None

    portal = canonicalize_portal_label(match.group(1))
    contact = normalize_text(match.group(2))
    return portal, contact


def extract_phone(text: str) -> str | None:
    if not text:
        return None

    matches = re.findall(r"(\+34[\s-]?\d{3}[\s-]?\d{2,3}[\s-]?\d{2}[\s-]?\d{2}|\b\d{9}\b)", text)
    if not matches:
        return None

    return normalize_text(matches[0])


def clean_contact_name(value: str | None) -> str | None:
    text = normalize_text(value)
    if not text:
        return None

    # Quita teléfonos
    text = re.sub(r"\+34[\s-]?\d{3}[\s-]?\d{2,3}[\s-]?\d{2}[\s-]?\d{2}", " ", text)
    text = re.sub(r"\b\d{9}\b", " ", text)

    # Quita números largos, ids y trozos muy técnicos
    text = re.sub(r"\b\d{4,}\b", " ", text)

    tokens = text.split()
    tokens = [
        tok for tok in tokens
        if normalize_text_key(tok) not in GENERIC_NAME_TOKENS
        and "active" not in normalize_text_key(tok)
        and "none" not in normalize_text_key(tok)
    ]

    if not tokens:
        return None

    # Corta antes de que empiece ruido largo
    cleaned = " ".join(tokens[:4]).strip()
    if len(cleaned) < 2:
        return None

    return cleaned


def contains_suspicious_noise(text: str | None) -> bool:
    text_l = normalize_text_key(text)
    if not text_l:
        return False
    return any(token in text_l for token in SUSPICIOUS_TEXT_SNIPPETS)


def has_event_signal(
    title: str | None,
    fallback_text: str | None,
    raw_event_type: str | None,
) -> bool:
    combined = " ".join(
        part for part in (title or "", fallback_text or "", raw_event_type or "") if part
    ).lower()

    return any(token in combined for token in EVENT_HINTS)


def has_strong_listing_identity(
    listing_url: str | None,
    source_listing_id: str | None,
    portal: str | None,
    contact_phone: str | None,
    address_raw: str | None,
) -> bool:
    if listing_url:
        return True

    if source_listing_id:
        return True

    if portal and contact_phone:
        return True

    if portal and address_raw:
        return True

    return False


def stringify_node(value: Any) -> str:
    if value is None:
        return ""

    if isinstance(value, (str, int, float, bool)):
        return str(value)

    if isinstance(value, dict):
        parts = [stringify_node(v) for v in value.values()]
        return " ".join(p for p in parts if p)

    if isinstance(value, list):
        parts = [stringify_node(v) for v in value]
        return " ".join(p for p in parts if p)

    return str(value)


def iter_dicts(node: Any):
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from iter_dicts(value)
    elif isinstance(node, list):
        for item in node:
            yield from iter_dicts(item)


def pick_first_value(record: dict, keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in record and record.get(key) not in (None, "", []):
            return record.get(key)

    lowered = {str(k).lower(): v for k, v in record.items()}
    for key in keys:
        if key.lower() in lowered and lowered[key.lower()] not in (None, "", []):
            return lowered[key.lower()]

    return None


def find_phone_in_record(record: dict) -> str | None:
    direct = pick_first_value(record, PHONE_KEYS)
    if direct:
        return extract_phone(str(direct))

    for key in ("contact", "seller", "broker", "agent"):
        nested = record.get(key)
        if isinstance(nested, dict):
            direct = pick_first_value(nested, PHONE_KEYS)
            if direct:
                return extract_phone(str(direct))

    text = stringify_node(record)
    return extract_phone(text)


def find_contact_name_in_record(record: dict) -> str | None:
    direct = pick_first_value(record, CONTACT_NAME_KEYS)
    if direct:
        cleaned = clean_contact_name(str(direct))
        if cleaned:
            return cleaned

    for key in ("contact", "seller", "broker", "agent"):
        nested = record.get(key)
        if isinstance(nested, dict):
            for nested_key in ("name", "fullName", "displayName"):
                value = nested.get(nested_key)
                if value:
                    cleaned = clean_contact_name(str(value))
                    if cleaned:
                        return cleaned

    # Fallback: intenta sacar nombre antes del teléfono en cadenas mezcladas
    text = normalize_text(stringify_node(record))
    if not text:
        return None

    text = re.sub(r"\+34[\s-]?\d{3}[\s-]?\d{2,3}[\s-]?\d{2}[\s-]?\d{2}", " |PHONE| ", text)
    text = re.sub(r"\b\d{9}\b", " |PHONE| ", text)
    before_phone = text.split("|PHONE|")[0].strip()
    cleaned = clean_contact_name(before_phone)
    return cleaned


def build_listing_fingerprint(item: dict) -> str:
    listing_url = normalize_url(item.get("listing_url"))
    portal_key = normalize_portal_key(item.get("portal"))
    phone_key = normalize_phone_key(item.get("contact_phone"))
    name_key = normalize_name_key(item.get("contact_name"))
    address_key = build_address_fragment_key(item.get("address_raw"))
    source_listing_id = normalize_text_key(item.get("source_listing_id"))
    current_price_key = price_key(item.get("current_price_eur"))
    title_key = normalize_text_key(item.get("title"))

    if listing_url:
        return f"url|{listing_url}"

    if source_listing_id and portal_key:
        return f"portal_listing_id|{portal_key}|{source_listing_id}"

    if phone_key and portal_key:
        return f"portal_phone|{portal_key}|{phone_key}"

    if phone_key:
        return f"phone|{phone_key}"

    if portal_key and name_key and address_key:
        return f"portal_name_addr|{portal_key}|{name_key}|{address_key}"

    if name_key and address_key:
        return f"name_addr|{name_key}|{address_key}"

    if portal_key and address_key and current_price_key != "na":
        return f"portal_addr_price|{portal_key}|{address_key}|{current_price_key}"

    if address_key and current_price_key != "na":
        return f"addr_price|{address_key}|{current_price_key}"

    if title_key and current_price_key != "na":
        return f"title_price|{title_key}|{current_price_key}"

    if title_key:
        return f"title|{title_key}"

    raw_text_key = normalize_text_key(item.get("raw_text"))
    if raw_text_key:
        return f"raw|{raw_text_key[:120]}"

    return "unknown"


def build_source_uid(item: dict) -> str:
    source_event_id = normalize_text_key(item.get("source_event_id"))
    listing_fp = item.get("listing_fingerprint") or build_listing_fingerprint(item)
    event_type = normalize_text_key(item.get("event_type_guess")) or "history_item"
    event_key = item.get("event_date_key") or event_date_key(item.get("event_datetime"))
    current_price_key = price_key(item.get("current_price_eur"))
    previous_price_key = price_key(item.get("previous_price_eur"))

    if source_event_id:
        base = f"source_event|{source_event_id}"
    else:
        base = "|".join(
            [
                "event",
                listing_fp,
                event_type,
                event_key,
                current_price_key,
                previous_price_key,
            ]
        )

    return hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()


def normalize_network_item(record: dict, page_url: str, payload_url: str, page_number: int) -> dict | None:
    listing_url = normalize_url(str(pick_first_value(record, URL_KEYS) or ""))
    title = normalize_text(str(pick_first_value(record, TITLE_KEYS) or ""))
    address_raw = normalize_text(str(pick_first_value(record, ADDRESS_KEYS) or ""))
    portal = canonicalize_portal_label(str(pick_first_value(record, PORTAL_KEYS) or ""))
    current_price = parse_price_value(pick_first_value(record, CURRENT_PRICE_KEYS))
    previous_price = parse_price_value(pick_first_value(record, PREVIOUS_PRICE_KEYS))
    event_datetime = parse_datetime_value(pick_first_value(record, DATETIME_KEYS))
    raw_event_type = normalize_text(str(pick_first_value(record, EVENT_TYPE_KEYS) or ""))
    source_event_id = normalize_text(str(pick_first_value(record, SOURCE_EVENT_ID_KEYS) or ""))
    source_listing_id = normalize_text(str(pick_first_value(record, SOURCE_LISTING_ID_KEYS) or ""))
    contact_name = find_contact_name_in_record(record)
    contact_phone = find_phone_in_record(record)

    fallback_text = normalize_text(stringify_node(record)) or ""
    noise_probe = " ".join(
        part for part in (payload_url, listing_url, title, fallback_text) if part
    ).lower()

    if any(token in noise_probe for token in NETWORK_NOISE_HINTS):
        return None

    if contains_suspicious_noise(fallback_text):
        return None

    if not title:
        title = normalize_text(fallback_text[:500])

    if not address_raw:
        address_raw = extract_address_from_title(title)

    if current_price is None:
        current_price, previous_price_fallback = parse_price_from_fallback_text(fallback_text)
        previous_price = previous_price or previous_price_fallback

    if not portal and fallback_text:
        portal, inferred_contact = extract_portal_and_contact(fallback_text)
        inferred_clean = clean_contact_name(inferred_contact)
        if inferred_clean:
            contact_name = contact_name or inferred_clean

    if not contact_phone and fallback_text:
        contact_phone = extract_phone(fallback_text)

    contact_name = clean_contact_name(contact_name)

    contextual_event_type = infer_event_type_from_context_urls(page_url, payload_url)
    event_type_guess = infer_event_type(title, fallback_text, raw_event_type)

    if contextual_event_type and (event_type_guess == "history_item" or not raw_event_type):
        event_type_guess = contextual_event_type

    strong_identity = has_strong_listing_identity(
        listing_url=listing_url,
        source_listing_id=source_listing_id,
        portal=portal,
        contact_phone=contact_phone,
        address_raw=address_raw,
    )

    event_signal = has_event_signal(
        title=title,
        fallback_text=fallback_text,
        raw_event_type=raw_event_type or contextual_event_type,
    )

    signal_count = 0
    if listing_url:
        signal_count += 1
    if source_listing_id:
        signal_count += 1
    if current_price is not None:
        signal_count += 1
    if title and len(title) >= 15:
        signal_count += 1
    if address_raw:
        signal_count += 1
    if portal:
        signal_count += 1
    if contact_phone:
        signal_count += 1
    if contact_name:
        signal_count += 1

    if not strong_identity:
        return None

    if not event_signal and event_type_guess == "history_item":
        return None

    if signal_count < 3:
        return None

    item = {
        "history_type": normalize_text(raw_event_type) or contextual_event_type or "new",
        "event_type_guess": event_type_guess,
        "event_datetime": event_datetime,
        "title": title,
        "address_raw": address_raw,
        "listing_url": listing_url,
        "portal": portal,
        "contact_name": contact_name,
        "contact_phone": contact_phone,
        "current_price_eur": current_price,
        "previous_price_eur": previous_price,
        "page_number": page_number,
        "raw_text": fallback_text,
        "source_event_id": source_event_id,
        "source_listing_id": source_listing_id,
    }

    item["listing_fingerprint"] = build_listing_fingerprint(item)
    item["event_date_key"] = event_date_key(item["event_datetime"])
    item["source_uid"] = build_source_uid(item)

    item["raw_payload_json"] = json.dumps(
        {
            "page_url": page_url,
            "payload_url": payload_url,
            "record": record,
            "meta": {
                "listing_fingerprint": item["listing_fingerprint"],
                "event_date_key": item["event_date_key"],
                "source_event_id": item.get("source_event_id"),
                "source_listing_id": item.get("source_listing_id"),
                "contextual_event_type": contextual_event_type,
            },
        },
        ensure_ascii=False,
        default=str,
    )

    return item


def parse_network_payload(payload: dict, page_url: str, page_number: int) -> tuple[list[dict], int | None]:
    results: list[dict] = []
    seen_uids: set[str] = set()

    data_root = payload.get("data", payload)
    total_from_this_payload = None

    listing_like_count = 0
    for record in iter_dicts(data_root):
        item = normalize_network_item(
            record,
            page_url=page_url,
            payload_url=payload.get("url", ""),
            page_number=page_number,
        )
        if not item:
            continue

        listing_like_count += 1

        if item["source_uid"] in seen_uids:
            continue

        seen_uids.add(item["source_uid"])
        results.append(item)

    if listing_like_count >= 3:
        total_from_this_payload = extract_total_from_payload(data_root)

    return results, total_from_this_payload


def score_payload(url: str, data: Any) -> int:
    score = 0
    url_l = url.lower()

    if any(hint in url_l for hint in JSON_URL_HINTS):
        score += 3

    data_root = data.get("data", data) if isinstance(data, dict) else data

    listing_like_count = 0
    for record in iter_dicts(data_root):
        listing_like = normalize_network_item(
            record,
            page_url=url,
            payload_url=url,
            page_number=1,
        )
        if listing_like:
            listing_like_count += 1
            score += 4

        if listing_like_count >= 5:
            break

    if listing_like_count == 0:
        return 0

    if extract_total_from_payload(data_root) is not None:
        score += 2

    return score


def extract_total_from_payload(data: Any) -> int | None:
    best_total: int | None = None

    for record in iter_dicts(data):
        for key in TOTAL_KEYS:
            value = record.get(key)
            if isinstance(value, int):
                if best_total is None or value > best_total:
                    best_total = value
            elif isinstance(value, str) and value.isdigit():
                numeric = int(value)
                if best_total is None or numeric > best_total:
                    best_total = numeric

    return best_total


def find_card_container(a_tag):
    node = a_tag
    for _ in range(8):
        if node is None:
            break

        text = normalize_text(node.get_text(" ", strip=True)) or ""
        if len(text) > 60 and ("€" in text or "Idealista" in text or "Fotocasa" in text):
            return node

        node = node.parent

    return a_tag.parent


def parse_history_page(html: str, page_url: str, page_number: int) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    results: list[dict] = []
    seen_uids: set[str] = set()
    contextual_event_type = infer_event_type_from_context_urls(page_url)

    for a_tag in soup.find_all("a", href=True):
        title = normalize_text(a_tag.get_text(" ", strip=True))
        if not title:
            continue

        title_l = title.lower()
        if all(
            token not in title_l
            for token in ("en venta", "bajada de precio", "subida de precio", "nuevo", "vendido")
        ):
            continue

        href = normalize_url(a_tag.get("href"))
        container = find_card_container(a_tag)
        card_text = normalize_text(container.get_text("\n", strip=True)) if container else title
        if not card_text:
            continue

        if contains_suspicious_noise(card_text):
            continue

        current_price, previous_price = parse_price_from_fallback_text(card_text)
        portal, contact_name = extract_portal_and_contact(card_text)
        event_datetime = parse_date_from_card(card_text)
        address_raw = extract_address_from_title(title)
        contact_phone = extract_phone(card_text)
        event_type_guess = infer_event_type(title, card_text, contextual_event_type or "new")

        if contextual_event_type and event_type_guess == "history_item":
            event_type_guess = contextual_event_type

        if not has_strong_listing_identity(
            listing_url=href,
            source_listing_id=None,
            portal=portal,
            contact_phone=contact_phone,
            address_raw=address_raw,
        ):
            continue

        if not has_event_signal(title, card_text, contextual_event_type or "new") and event_type_guess == "history_item":
            continue

        item = {
            "history_type": contextual_event_type or "new",
            "event_type_guess": event_type_guess,
            "event_datetime": event_datetime,
            "title": title,
            "address_raw": address_raw,
            "listing_url": href,
            "portal": portal,
            "contact_name": clean_contact_name(contact_name),
            "contact_phone": contact_phone,
            "current_price_eur": current_price,
            "previous_price_eur": previous_price,
            "page_number": page_number,
            "raw_text": card_text,
            "source_event_id": None,
            "source_listing_id": None,
        }

        item["listing_fingerprint"] = build_listing_fingerprint(item)
        item["event_date_key"] = event_date_key(item["event_datetime"])
        item["source_uid"] = build_source_uid(item)
        item["raw_payload_json"] = json.dumps(
            {
                "page_url": page_url,
                "title": title,
                "card_text": card_text,
                "meta": {
                    "listing_fingerprint": item["listing_fingerprint"],
                    "event_date_key": item["event_date_key"],
                    "contextual_event_type": contextual_event_type,
                },
            },
            ensure_ascii=False,
        )

        if item["source_uid"] in seen_uids:
            continue

        seen_uids.add(item["source_uid"])
        results.append(item)

    return results


def extract_total_count(page_text: str) -> int | None:
    match = re.search(r"(\d+)\s+anuncios", page_text, flags=re.IGNORECASE)
    if not match:
        return None
    return int(match.group(1))


def save_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8", errors="ignore")


def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def save_failure_debug(
    *,
    debug_dir: Path,
    target_url: str,
    final_url: str,
    sync_mode: str,
    error_message: str,
    extractor_used: str = "none",
    pages_seen: int = 0,
    items_seen: int = 0,
    total_expected: int | None = None,
    candidate_payloads: int = 0,
    captured_payloads_total: int = 0,
    warnings: list[str] | None = None,
    body_text: str | None = None,
    page_html: str | None = None,
) -> None:
    html_dir = debug_dir / "html"
    screenshots_dir = debug_dir / "screenshots"
    text_dir = debug_dir / "text"

    if page_html:
        save_text(html_dir / "failure_page.html", page_html)
    if body_text:
        save_text(text_dir / "failure_page.txt", body_text)

    save_json(
        debug_dir / "run_summary.json",
        {
            "target_url": target_url,
            "final_url": final_url,
            "pages_seen": pages_seen,
            "items_seen": items_seen,
            "total_expected": total_expected,
            "coverage_gap": (
                max(total_expected - items_seen, 0)
                if isinstance(total_expected, int)
                else None
            ),
            "extractor_used": extractor_used,
            "sync_mode": sync_mode,
            "candidate_payloads": candidate_payloads,
            "captured_payloads_total": captured_payloads_total,
            "warnings": warnings or [],
            "error_message": error_message,
            "failed": True,
            "screenshots_dir": str(screenshots_dir),
        },
    )


def get_page_signature(page) -> str:
    try:
        body_text = page.locator("body").inner_text(timeout=8000)
    except Exception:
        body_text = page.url

    normalized = normalize_text_key(body_text) or page.url
    normalized = normalized[:4000]
    return hashlib.sha1(normalized.encode("utf-8", errors="ignore")).hexdigest()


def wait_for_page_change(
    page,
    previous_url: str,
    previous_signature: str,
    *,
    wait_between_page_checks_ms: int = CASAFARI_WAIT_BETWEEN_PAGE_CHECKS_MS,
    page_change_max_wait_ms: int = CASAFARI_PAGE_CHANGE_MAX_WAIT_MS,
) -> bool:
    elapsed = 0

    while elapsed < page_change_max_wait_ms:
        page.wait_for_timeout(wait_between_page_checks_ms)
        elapsed += wait_between_page_checks_ms

        current_url = page.url
        current_signature = get_page_signature(page)

        if current_url != previous_url or current_signature != previous_signature:
            return True

    return False


def click_next_page(
    page,
    current_page_number: int,
    *,
    wait_after_pagination_ms: int = CASAFARI_WAIT_AFTER_PAGINATION_MS,
    wait_between_page_checks_ms: int = CASAFARI_WAIT_BETWEEN_PAGE_CHECKS_MS,
    page_change_max_wait_ms: int = CASAFARI_PAGE_CHANGE_MAX_WAIT_MS,
    scroll_wait_ms: int = 500,
) -> bool:
    previous_url = page.url
    previous_signature = get_page_signature(page)

    try:
        page.mouse.wheel(0, 4000)
        page.wait_for_timeout(scroll_wait_ms)
    except Exception:
        pass

    next_page_number = current_page_number + 1

    candidates = [
        "a[rel='next']",
        "button:has-text('Siguiente')",
        "a:has-text('Siguiente')",
        "button:has-text('Next')",
        "a:has-text('Next')",
        "[aria-label*='siguiente' i]",
        "[aria-label*='next' i]",
        "li.next a",
        ".pagination-next a",
        ".next a",
        f"nav a:has-text('{next_page_number}')",
        f"ul a:has-text('{next_page_number}')",
        f"li a:has-text('{next_page_number}')",
        "text=›",
        "text=>",
    ]

    for selector in candidates:
        locator = page.locator(selector)
        try:
            count = locator.count()
        except Exception:
            count = 0

        if count <= 0:
            continue

        try:
            candidate = locator.first
            if not candidate.is_visible():
                continue

            candidate.click(timeout=3000)

            try:
                page.wait_for_load_state("domcontentloaded", timeout=10000)
            except PlaywrightTimeoutError:
                pass

            page.wait_for_timeout(wait_after_pagination_ms)

            if wait_for_page_change(
                page,
                previous_url,
                previous_signature,
                wait_between_page_checks_ms=wait_between_page_checks_ms,
                page_change_max_wait_ms=page_change_max_wait_ms,
            ):
                return True
        except Exception:
            continue

    return False


class CasafariHistoryConnector:
    def __init__(self, progress_callback=None) -> None:
        self.progress_callback = progress_callback

    def _emit(self, message: str, current: int = 0, total: int = 0) -> None:
        if self.progress_callback:
            self.progress_callback(message, current, total)

    def fetch_history(
        self,
        from_dt: datetime,
        to_dt: datetime,
        *,
        sync_mode: str = "balanced",
    ) -> dict:
        options = resolve_fetch_options(sync_mode)
        base_url = load_history_base_url()
        target_url = build_history_url(base_url, from_dt, to_dt)

        run_id = utc_now_naive().strftime("%Y%m%d_%H%M%S")
        debug_dir = CASAFARI_DEBUG_BASE_DIR / run_id
        html_dir = debug_dir / "html"
        screenshots_dir = debug_dir / "screenshots"
        text_dir = debug_dir / "text"
        network_dir = debug_dir / "network"

        for path in (debug_dir, html_dir, screenshots_dir, text_dir, network_dir):
            path.mkdir(parents=True, exist_ok=True)

        has_profile = CASAFARI_PROFILE_DIR.exists() and any(CASAFARI_PROFILE_DIR.iterdir())
        has_storage_state = CASAFARI_STORAGE_STATE_PATH.exists()
        if not has_profile and not has_storage_state:
            save_failure_debug(
                debug_dir=debug_dir,
                target_url=target_url,
                final_url=target_url,
                sync_mode=options.mode,
                error_message="No existe una sesión Casafari preparada",
                warnings=["missing_session"],
            )
            raise FileNotFoundError(
                "No existe una sesión Casafari preparada. Pulsa primero 'Preparar sesión' en la app."
            )

        all_items: list[dict] = []
        seen_uids: set[str] = set()
        page_number = 1
        total_expected = 0
        extractor_used = "none"
        captured_payloads: list[dict] = []
        final_url = target_url
        warnings: list[str] = []

        with sync_playwright() as p:
            if has_profile:
                context = p.chromium.launch_persistent_context(
                    str(CASAFARI_PROFILE_DIR),
                    headless=options.headless,
                    locale="es-ES",
                )
                page = context.pages[0] if context.pages else context.new_page()
            else:
                browser = p.chromium.launch(headless=options.headless)
                context = browser.new_context(
                    locale="es-ES",
                    storage_state=str(CASAFARI_STORAGE_STATE_PATH),
                )
                page = context.new_page()

            def on_response(response) -> None:
                try:
                    headers = response.headers or {}
                    content_type = headers.get("content-type", "")
                    if "json" not in content_type.lower():
                        return

                    text = response.text()
                    data = json.loads(text)
                    score = score_payload(response.url, data)

                    payload_info = {
                        "url": response.url,
                        "status": response.status,
                        "content_type": content_type,
                        "score": score,
                        "data": data,
                    }
                    captured_payloads.append(payload_info)

                    index = len(captured_payloads)
                    save_json(network_dir / f"response_{index:03d}.json", payload_info)
                except Exception:
                    return

            page.on("response", on_response)

            self._emit("Abriendo Casafari...", 0, 0)

            try:
                page.goto(target_url, wait_until="domcontentloaded", timeout=45000)
            except PlaywrightTimeoutError:
                page.goto(target_url, wait_until="commit", timeout=45000)

            page.wait_for_timeout(options.wait_after_goto_ms)
            final_url = page.url

            try:
                body_text_after_goto = page.locator("body").inner_text(timeout=15000)
            except Exception:
                body_text_after_goto = ""

            try:
                page_html_after_goto = page.content()
            except Exception:
                page_html_after_goto = ""

            try:
                page.screenshot(
                    path=str(screenshots_dir / "page_000_after_goto.png"),
                    full_page=True,
                )
            except Exception:
                pass

            if "login" in final_url.lower():
                save_failure_debug(
                    debug_dir=debug_dir,
                    target_url=target_url,
                    final_url=final_url,
                    sync_mode=options.mode,
                    error_message="Casafari ha redirigido a login tras abrir la URL de historial",
                    warnings=["redirected_to_login"],
                    body_text=body_text_after_goto,
                    page_html=page_html_after_goto,
                )
                context.close()
                raise RuntimeError(
                    "Casafari ha redirigido a login durante el sync. "
                    f"Prepara la sesión de nuevo y revisa el debug en {debug_dir}."
                )

            if not looks_like_history_body(body_text_after_goto):
                save_failure_debug(
                    debug_dir=debug_dir,
                    target_url=target_url,
                    final_url=final_url,
                    sync_mode=options.mode,
                    error_message="La página abierta no parece la vista de historial de Casafari",
                    warnings=["history_not_ready"],
                    body_text=body_text_after_goto,
                    page_html=page_html_after_goto,
                )
                context.close()
                raise RuntimeError(
                    f"Casafari no dejó la vista de historial lista tras abrir la URL. Debug: {debug_dir}"
                )

            payload_cursor = 0

            while page_number <= options.max_pages:
                try:
                    page.wait_for_load_state("domcontentloaded", timeout=10000)
                except PlaywrightTimeoutError:
                    pass

                page.wait_for_timeout(options.settle_wait_ms)

                try:
                    page.mouse.wheel(0, 3000)
                    page.wait_for_timeout(options.scroll_wait_ms)
                    page.mouse.wheel(0, -1500)
                    page.wait_for_timeout(options.scroll_wait_ms)
                except Exception:
                    pass

                html = page.content()
                body_text = page.locator("body").inner_text(timeout=10000)

                save_text(html_dir / f"page_{page_number:03d}.html", html)
                save_text(text_dir / f"page_{page_number:03d}.txt", body_text)
                page.screenshot(
                    path=str(screenshots_dir / f"page_{page_number:03d}.png"),
                    full_page=True,
                )

                new_payloads = captured_payloads[payload_cursor:]
                payload_cursor = len(captured_payloads)

                page_items_network: list[dict] = []
                payload_totals: list[int] = []

                useful_payloads = sorted(
                    (p for p in new_payloads if p.get("score", 0) > 0),
                    key=lambda p: p.get("score", 0),
                    reverse=True,
                )

                for payload in useful_payloads:
                    parsed_items, parsed_total = parse_network_payload(
                        payload,
                        page_url=page.url,
                        page_number=page_number,
                    )

                    if parsed_total is not None:
                        payload_totals.append(parsed_total)

                    for item in parsed_items:
                        if item["source_uid"] in seen_uids:
                            continue
                        seen_uids.add(item["source_uid"])
                        page_items_network.append(item)

                page_added = 0

                if len(page_items_network) >= 3:
                    extractor_used = "network"
                    all_items.extend(page_items_network)
                    page_added = len(page_items_network)
                else:
                    if page_items_network:
                        warnings.append(
                            f"Payload network débil en página {page_number}. Se usa DOM como fallback."
                        )
                    page_items_dom = []
                    for item in parse_history_page(html, page.url, page_number):
                        if item["source_uid"] in seen_uids:
                            continue
                        seen_uids.add(item["source_uid"])
                        page_items_dom.append(item)

                    if page_items_dom and extractor_used == "none":
                        extractor_used = "dom"

                    all_items.extend(page_items_dom)
                    page_added = len(page_items_dom)

                if payload_totals:
                    total_expected = max(total_expected, max(payload_totals))

                self._emit(
                    f"Procesando página {page_number}...",
                    len(all_items),
                    total_expected,
                )

                if total_expected and len(all_items) >= total_expected:
                    break

                if page_added == 0 and page_number > 1:
                    warnings.append(
                        f"La página {page_number} no añadió items nuevos. Se corta para evitar bucles."
                    )
                    break

                if not click_next_page(
                    page,
                    page_number,
                    wait_after_pagination_ms=options.wait_after_pagination_ms,
                    wait_between_page_checks_ms=options.wait_between_page_checks_ms,
                    page_change_max_wait_ms=options.page_change_max_wait_ms,
                    scroll_wait_ms=options.scroll_wait_ms,
                ):
                    if total_expected and len(all_items) < total_expected:
                        warnings.append(
                            f"No se pudo avanzar a la página siguiente. "
                            f"Extraídos {len(all_items)} de {total_expected} esperados."
                        )
                    break

                final_url = page.url
                page_number += 1

            save_json(
                debug_dir / "run_summary.json",
                {
                    "target_url": target_url,
                    "final_url": final_url,
                    "pages_seen": page_number,
                    "items_seen": len(all_items),
                    "total_expected": total_expected,
                    "coverage_gap": max(total_expected - len(all_items), 0) if total_expected else None,
                    "extractor_used": extractor_used,
                    "sync_mode": options.mode,
                    "candidate_payloads": len([p for p in captured_payloads if p.get("score", 0) > 0]),
                    "captured_payloads_total": len(captured_payloads),
                    "warnings": warnings,
                },
            )

            try:
                context.storage_state(path=str(CASAFARI_STORAGE_STATE_PATH))
            except Exception:
                pass

            context.close()

        return {
            "target_url": target_url,
            "final_url": final_url,
            "items": all_items,
            "debug_dir": str(debug_dir),
            "sync_mode": options.mode,
            "extractor_used": extractor_used,
            "pages_seen": page_number,
            "candidate_payload_count": len([p for p in captured_payloads if p.get("score", 0) > 0]),
            "captured_payload_count": len(captured_payloads),
            "total_expected": total_expected,
            "warnings": warnings,
        }
