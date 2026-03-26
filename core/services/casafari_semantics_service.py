from __future__ import annotations

import re
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from core.normalization.phones import normalize_phone
from core.normalization.text import normalize_text, normalize_text_key
from db.models.contact import Contact
from db.models.listing import Listing
from db.models.raw_history_item import RawHistoryItem
from db.models.casafari_event_link import CasafariEventLink

STREET_HINTS = (
    "calle",
    "cl",
    "avenida",
    "av",
    "paseo",
    "ps",
    "plaza",
    "pl",
    "carretera",
    "camino",
    "via",
    "ronda",
    "travesia",
)

EVENT_CONTEXT_TYPE_MAP = {
    "new": "listing_detected",
    "listing_detected": "listing_detected",
    "listingdetected": "listing_detected",
    "pricedrop": "price_drop",
    "price_drop": "price_drop",
    "price-drop": "price_drop",
    "decrease": "price_drop",
    "priceraise": "price_raise",
    "price_raise": "price_raise",
    "price-raise": "price_raise",
    "increase": "price_raise",
    "reserved": "reserved",
    "notavailable": "not_available",
    "not_available": "not_available",
    "sold": "sold",
    "expired": "expired",
}

GENERIC_CONTACT_KEYS = {
    "anuncio particular",
    "particular",
    "profesional",
    "inmobiliaria",
    "agencia",
    "anunciante",
    "propietario",
}


def ensure_utc_naive(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def infer_event_type_from_context_urls(*urls: str | None) -> str | None:
    for url in urls:
        if not url:
            continue

        try:
            parsed = urlparse(url)
            query = parse_qs(parsed.query)
        except Exception:
            continue

        raw_value = None
        for key in ("historyType", "history_type", "type"):
            value = query.get(key, [None])[0]
            if value:
                raw_value = value
                break

        if not raw_value:
            continue

        key = normalize_text_key(raw_value).replace("-", "_")
        key = key.replace("__", "_")
        compact = key.replace("_", "")

        if key in EVENT_CONTEXT_TYPE_MAP:
            return EVENT_CONTEXT_TYPE_MAP[key]
        if compact in EVENT_CONTEXT_TYPE_MAP:
            return EVENT_CONTEXT_TYPE_MAP[compact]

    return None


def classify_address_semantics(address_raw: str | None) -> dict[str, str | None]:
    text = normalize_text(address_raw)
    if not text:
        return {
            "address_precision": "unknown",
            "zone_like_label": None,
        }

    key = normalize_text_key(text)
    tokens = text.split()

    has_number = bool(re.search(r"\b\d+[a-z]?\b", key))
    has_street_hint = any(f" {hint} " in f" {key} " for hint in STREET_HINTS)

    if "barrio" in key or "district" in key or " distrito " in f" {key} ":
        zone_label = text.split("-")[0].strip() if "-" in text else text
        return {
            "address_precision": "zone_like",
            "zone_like_label": zone_label,
        }

    if not has_number and "-" in text and len(tokens) <= 6:
        zone_label = text.split("-")[0].strip()
        return {
            "address_precision": "zone_like",
            "zone_like_label": zone_label,
        }

    if has_street_hint and has_number:
        return {
            "address_precision": "precise",
            "zone_like_label": None,
        }

    if has_street_hint or len(tokens) >= 3:
        return {
            "address_precision": "partial",
            "zone_like_label": None,
        }

    if len(tokens) <= 2:
        return {
            "address_precision": "zone_like",
            "zone_like_label": text,
        }

    return {
        "address_precision": "unknown",
        "zone_like_label": None,
    }


def classify_price_semantics(
    current_price_eur: float | None,
    previous_price_eur: float | None,
    raw_text: str | None,
) -> dict[str, str | None]:
    if current_price_eur is None:
        return {
            "price_confidence": "none",
            "price_source": "none",
        }

    text = normalize_text(raw_text) or ""
    text_key = normalize_text_key(text)

    has_euro = "€" in text or " eur " in f" {text_key} "
    has_price_keyword = any(
        token in text_key
        for token in ("precio", "price", "venta", "sale", "bajada", "subida")
    )

    if has_euro:
        confidence = "high"
        source = "eur_text"
    elif has_price_keyword:
        confidence = "medium"
        source = "contextual_fallback"
    else:
        confidence = "low"
        source = "structured_or_weak_fallback"

    if current_price_eur < 30000 or current_price_eur > 50000000:
        confidence = "low"

    if previous_price_eur is not None and confidence == "medium":
        confidence = "high"

    return {
        "price_confidence": confidence,
        "price_source": source,
    }


def classify_phone_profile(session: Session, phone_raw: str | None) -> dict[str, int | str | None]:
    phone_norm = normalize_phone(phone_raw)
    if not phone_norm:
        return {
            "phone_profile": "unknown",
            "phone_listing_count": 0,
            "phone_asset_count": 0,
            "phone_portal_count": 0,
        }

    listing_count = int(
        session.scalar(
            select(func.count(Listing.id))
            .join(Contact, Listing.contact_id == Contact.id)
            .where(Contact.phone_norm == phone_norm)
        )
        or 0
    )

    asset_count = int(
        session.scalar(
            select(func.count(func.distinct(Listing.asset_id)))
            .join(Contact, Listing.contact_id == Contact.id)
            .where(Contact.phone_norm == phone_norm)
        )
        or 0
    )

    portal_count = int(
        session.scalar(
            select(func.count(func.distinct(Listing.source_portal)))
            .join(Contact, Listing.contact_id == Contact.id)
            .where(Contact.phone_norm == phone_norm)
        )
        or 0
    )

    if listing_count == 0:
        profile = "unknown"
    elif listing_count == 1 and asset_count <= 1 and portal_count <= 1:
        profile = "owner_like"
    elif listing_count >= 4 or asset_count >= 3 or portal_count >= 3:
        profile = "broker_like"
    else:
        profile = "unknown"

    return {
        "phone_profile": profile,
        "phone_listing_count": listing_count,
        "phone_asset_count": asset_count,
        "phone_portal_count": portal_count,
    }


def classify_match_confidence_band(match_status: str | None, match_score: float | None) -> str:
    if match_status is None:
        return "none"

    if match_status == "resolved":
        if match_score is not None and match_score >= 90:
            return "high"
        if match_score is not None and match_score >= 75:
            return "medium"
        return "low"

    if match_status == "ambiguous":
        if match_score is not None and match_score >= 70:
            return "medium"
        return "low"

    if match_status == "unresolved":
        return "low"

    return "none"


def safe_event_datetime(item: RawHistoryItem) -> datetime:
    return (
        ensure_utc_naive(item.event_datetime)
        or ensure_utc_naive(item.captured_at)
        or ensure_utc_naive(item.created_at)
        or datetime.now(timezone.utc).replace(tzinfo=None)
    )


def infer_match_reason_taxonomy(
    session: Session,
    item: RawHistoryItem,
    link: CasafariEventLink,
) -> str:
    note = normalize_text_key(link.match_note)
    status = link.match_status or ""
    address_meta = classify_address_semantics(item.address_raw)
    phone_meta = classify_phone_profile(session, item.contact_phone)
    event_dt = safe_event_datetime(item)
    now_dt = datetime.now(timezone.utc).replace(tzinfo=None)

    if status == "resolved":
        if (link.match_score or 0) >= 90:
            return "resolved_strong"
        return "resolved_soft"

    if status == "ambiguous":
        return "ambiguous_multiple_candidates"

    if status == "pending":
        return "pending_review"

    if "sin candidatos suficientes" in note:
        if item.event_type_guess == "listing_detected" and (now_dt - event_dt).days <= 10:
            return "not_in_csv_yet"
        return "no_candidates"

    if address_meta["address_precision"] == "zone_like" and not item.listing_url and not item.contact_phone:
        return "zone_only_address"

    if phone_meta["phone_profile"] == "broker_like":
        return "repeated_phone_conflict"

    if "precio lejos" in note:
        return "price_conflict"

    if not item.listing_url and not item.contact_phone and not item.address_raw:
        return "weak_identity"

    return "weak_identity"