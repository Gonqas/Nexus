from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from rapidfuzz import fuzz
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session, joinedload

from core.identity.listing_resolver import extract_external_id, find_existing_listing
from core.normalization.addresses import normalize_address_key
from core.normalization.phones import normalize_phone
from core.normalization.text import normalize_text_key
from core.services.casafari_semantics_service import (
    classify_address_semantics,
    classify_phone_profile,
)
from db.models.asset import Asset
from db.models.casafari_event_link import CasafariEventLink
from db.models.contact import Contact
from db.models.listing import Listing
from db.models.market_event import MarketEvent
from db.models.raw_history_item import RawHistoryItem

GENERIC_CONTACT_KEYS = {
    "anuncio particular",
    "particular",
    "profesional",
    "inmobiliaria",
    "agencia",
    "anunciante",
    "propietario",
}

STATUS_BY_EVENT_TYPE = {
    "listing_detected": "active",
    "reserved": "reserved",
    "not_available": "not_available",
    "sold": "sold",
    "expired": "expired",
}


@dataclass
class CandidateScore:
    listing: Listing
    score: float
    reasons: list[str]


def ensure_utc_naive(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def build_address_fragment_key(value: str | None) -> str | None:
    text = normalize_text_key(value)
    if not text:
        return None

    text = text.replace(",", " ").replace("-", " ").replace(".", " ")
    text = normalize_text_key(text)
    if not text:
        return None

    import re

    text = re.sub(r"\b\d+[a-z]?\b", " ", text)
    text = re.sub(
        r"\b(calle|cl|avenida|av|paseo|ps|plaza|pl|carretera|camino|via|ronda|travesia)\b",
        " ",
        text,
    )
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return None

    tokens = text.split()
    return " ".join(tokens[:4]) if tokens else None


def is_generic_contact_name(value: str | None) -> bool:
    key = normalize_text_key(value)
    if not key:
        return True
    return key in GENERIC_CONTACT_KEYS


def normalize_portal_key(value: str | None) -> str | None:
    return normalize_text_key(value)


def safe_event_datetime(item: RawHistoryItem) -> datetime:
    return (
        ensure_utc_naive(item.event_datetime)
        or ensure_utc_naive(item.captured_at)
        or ensure_utc_naive(item.created_at)
        or datetime.now(timezone.utc).replace(tzinfo=None)
    )


def price_distance_ratio(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or a <= 0 or b <= 0:
        return None
    return abs(a - b) / max(a, b)


def address_similarity(a: str | None, b: str | None) -> float:
    a_key = build_address_fragment_key(a)
    b_key = build_address_fragment_key(b)
    if not a_key or not b_key:
        return 0.0
    return max(float(fuzz.ratio(a_key, b_key)), float(fuzz.partial_ratio(a_key, b_key)))


def get_or_create_link(session: Session, item: RawHistoryItem) -> CasafariEventLink:
    link = session.scalar(
        select(CasafariEventLink).where(CasafariEventLink.raw_history_item_id == item.id)
    )
    if link is not None:
        return link

    link = CasafariEventLink(raw_history_item_id=item.id, match_status="pending")
    session.add(link)
    session.flush()
    return link


def get_or_create_contact_for_item(session: Session, item: RawHistoryItem) -> Contact | None:
    if not item.contact_phone and not item.contact_name:
        return None

    from core.services.import_service import get_or_create_contact

    name = None if is_generic_contact_name(item.contact_name) else item.contact_name
    contact = get_or_create_contact(
        session,
        phone_raw=item.contact_phone,
        name_raw=name,
    )
    session.flush()
    return contact


def candidate_listings_for_item(session: Session, item: RawHistoryItem) -> list[Listing]:
    candidates_by_id: dict[int, Listing] = {}
    address_meta = classify_address_semantics(item.address_raw)

    direct_listing = find_existing_listing(
        session=session,
        listing_url=item.listing_url,
        property_url=None,
        external_id=extract_external_id(item.listing_url),
        source_portal=item.portal,
        asset_id=None,
        contact_id=None,
    )
    if direct_listing is not None:
        direct_listing = session.scalar(
            select(Listing)
            .options(joinedload(Listing.asset), joinedload(Listing.contact))
            .where(Listing.id == direct_listing.id)
        )
        if direct_listing is not None:
            candidates_by_id[direct_listing.id] = direct_listing

    phone_norm = normalize_phone(item.contact_phone)
    if phone_norm:
        stmt = (
            select(Listing)
            .join(Contact, Listing.contact_id == Contact.id)
            .options(joinedload(Listing.asset), joinedload(Listing.contact))
            .where(Contact.phone_norm == phone_norm)
        )
        if item.portal:
            stmt = stmt.where(Listing.source_portal == item.portal)
        for listing in session.scalars(stmt.limit(150)).all():
            candidates_by_id[listing.id] = listing

    portal_key = normalize_portal_key(item.portal)
    addr_fragment = build_address_fragment_key(item.address_raw)
    if (portal_key or addr_fragment) and address_meta["address_precision"] != "zone_like":
        stmt = (
            select(Listing)
            .join(Asset, Listing.asset_id == Asset.id)
            .options(joinedload(Listing.asset), joinedload(Listing.contact))
        )
        if item.portal:
            stmt = stmt.where(Listing.source_portal == item.portal)
        for listing in session.scalars(stmt.limit(400)).all():
            asset_addr = listing.asset.address_raw if listing.asset else None
            if addr_fragment:
                sim = address_similarity(item.address_raw, asset_addr)
                if sim < 70:
                    continue
            candidates_by_id[listing.id] = listing

    if item.contact_name and not is_generic_contact_name(item.contact_name):
        name_key = normalize_text_key(item.contact_name)
        stmt = (
            select(Listing)
            .join(Contact, Listing.contact_id == Contact.id)
            .options(joinedload(Listing.asset), joinedload(Listing.contact))
            .where(Contact.name_norm == name_key)
        )
        if item.portal:
            stmt = stmt.where(Listing.source_portal == item.portal)
        for listing in session.scalars(stmt.limit(100)).all():
            candidates_by_id[listing.id] = listing

    return list(candidates_by_id.values())


def score_candidate(session: Session, item: RawHistoryItem, listing: Listing) -> CandidateScore:
    score = 0.0
    reasons: list[str] = []

    item_url = item.listing_url
    listing_url = listing.listing_url
    item_external_id = extract_external_id(item.listing_url)

    if item_url and listing_url and item_url == listing_url:
        score += 100
        reasons.append("listing_url exacta")

    if item_external_id and listing.external_id and item_external_id == listing.external_id:
        score += 95
        reasons.append("external_id exacto")

    if item.portal and listing.source_portal and normalize_portal_key(item.portal) == normalize_portal_key(listing.source_portal):
        score += 15
        reasons.append("portal exacto")

    item_phone_norm = normalize_phone(item.contact_phone)
    listing_phone_norm = listing.contact.phone_norm if listing.contact else None
    if item_phone_norm and listing_phone_norm and item_phone_norm == listing_phone_norm:
        phone_meta = classify_phone_profile(session, item.contact_phone)
        profile = phone_meta["phone_profile"]
        if profile == "owner_like":
            score += 45
            reasons.append("telefono exacto owner_like")
        elif profile == "unknown":
            score += 28
            reasons.append("telefono exacto")
        else:
            score += 10
            reasons.append("telefono exacto broker_like")

    if item.contact_name and listing.contact and listing.contact.name_raw and not is_generic_contact_name(item.contact_name):
        if normalize_text_key(item.contact_name) == normalize_text_key(listing.contact.name_raw):
            score += 8
            reasons.append("nombre contacto")

    asset_addr = listing.asset.address_raw if listing.asset else None
    similarity = address_similarity(item.address_raw, asset_addr)
    if similarity >= 96:
        score += 28
        reasons.append("direccion muy parecida")
    elif similarity >= 88:
        score += 18
        reasons.append("direccion parecida")
    elif similarity >= 78:
        score += 8
        reasons.append("direccion compatible")

    ratio = price_distance_ratio(item.current_price_eur, listing.price_eur)
    if ratio is not None:
        if ratio <= 0.05:
            score += 18
            reasons.append("precio muy cercano")
        elif ratio <= 0.12:
            score += 10
            reasons.append("precio cercano")
        elif ratio <= 0.20:
            score += 4
            reasons.append("precio compatible")
        else:
            score -= 10
            reasons.append("precio lejos")

    if listing.asset and item.address_raw:
        raw_exact_key = normalize_address_key(item.address_raw)
        if raw_exact_key and listing.asset.address_norm and raw_exact_key == listing.asset.address_norm:
            score += 12
            reasons.append("direccion core exacta")

    return CandidateScore(listing=listing, score=score, reasons=reasons)


def create_or_update_market_event(
    session: Session,
    item: RawHistoryItem,
    listing: Listing,
) -> tuple[MarketEvent, bool]:
    event_dt = safe_event_datetime(item)
    event_type = item.event_type_guess or "casafari_event"

    existing_events = list(
        session.scalars(
            select(MarketEvent).where(
                MarketEvent.listing_id == listing.id,
                MarketEvent.asset_id == listing.asset_id,
                MarketEvent.source_channel == "casafari",
                MarketEvent.event_type == event_type,
            )
        ).all()
    )

    for existing in existing_events:
        existing_dt = ensure_utc_naive(existing.event_datetime)
        same_day = existing_dt and event_dt and existing_dt.date() == event_dt.date()
        same_price_new = existing.price_new == item.current_price_eur
        same_price_old = existing.price_old == item.previous_price_eur
        if same_day and same_price_new and same_price_old:
            return existing, False

    event = MarketEvent(
        asset_id=listing.asset_id,
        listing_id=listing.id,
        event_type=event_type,
        event_datetime=event_dt,
        price_old=item.previous_price_eur,
        price_new=item.current_price_eur,
        status_new=STATUS_BY_EVENT_TYPE.get(event_type),
        source_channel="casafari",
        raw_text=f"{item.source_uid} | {item.raw_text or ''}",
    )
    session.add(event)
    session.flush()
    return event, True


def apply_item_to_listing(item: RawHistoryItem, listing: Listing, contact: Contact | None) -> None:
    event_dt = safe_event_datetime(item)
    listing_first_seen = ensure_utc_naive(listing.first_seen_at)
    listing_last_seen = ensure_utc_naive(listing.last_seen_at)

    if contact and listing.contact_id is None:
        listing.contact_id = contact.id

    if item.portal and not listing.source_portal:
        listing.source_portal = item.portal

    if item.listing_url and not listing.listing_url:
        listing.listing_url = item.listing_url
        listing.external_id = extract_external_id(item.listing_url) or listing.external_id

    if item.current_price_eur is not None:
        listing.price_eur = item.current_price_eur
        if listing.area_m2:
            listing.price_per_m2 = item.current_price_eur / listing.area_m2

    new_status = STATUS_BY_EVENT_TYPE.get(item.event_type_guess or "")
    if new_status:
        listing.status = new_status

    if listing_first_seen is None:
        listing.first_seen_at = event_dt

    if listing_last_seen is None or event_dt > listing_last_seen:
        listing.last_seen_at = event_dt


def build_match_strategy(reasons: list[str]) -> str:
    joined = " | ".join(reasons).lower()
    if "listing_url exacta" in joined or "external_id exacto" in joined:
        return "direct_anchor"
    if "telefono exacto owner_like" in joined and "portal exacto" in joined:
        return "phone_portal_owner"
    if "telefono exacto" in joined and "precio cercano" in joined:
        return "phone_price"
    return "candidate_scoring"


def derive_unresolved_reason(session: Session, item: RawHistoryItem, note: str, best_score: float | None = None) -> str:
    address_meta = classify_address_semantics(item.address_raw)
    phone_meta = classify_phone_profile(session, item.contact_phone)
    event_dt = safe_event_datetime(item)
    now_dt = datetime.now(timezone.utc).replace(tzinfo=None)
    note_key = normalize_text_key(note)

    if "sin candidatos suficientes" in note_key:
        if item.event_type_guess == "listing_detected" and (now_dt - event_dt).days <= 10:
            return "not_in_csv_yet"
        return "no_candidates"

    if address_meta["address_precision"] == "zone_like" and not item.listing_url and not item.contact_phone:
        return "zone_only_address"

    if phone_meta["phone_profile"] == "broker_like":
        return "repeated_phone_conflict"

    if "precio lejos" in note_key:
        return "price_conflict"

    if best_score is not None and best_score >= 55:
        return "ambiguous_multiple_candidates"

    return "weak_identity"


def resolve_raw_item(session: Session, item: RawHistoryItem) -> tuple[str, bool, str, float | None]:
    link = get_or_create_link(session, item)
    if link.match_status == "resolved" and link.market_event_id:
        return "resolved", False, link.match_strategy or "already_resolved", link.match_score

    contact = get_or_create_contact_for_item(session, item)
    candidates = candidate_listings_for_item(session, item)

    if not candidates:
        link.contact_id = contact.id if contact else None
        link.match_status = "unresolved"
        link.match_strategy = "no_candidates"
        reason = derive_unresolved_reason(
            session,
            item,
            "Sin candidatos suficientes en listings actuales",
        )
        link.match_note = f"reason={reason} | Sin candidatos suficientes en listings actuales"
        link.match_score = 0.0
        return "unresolved", False, "no_candidates", 0.0

    scored = [score_candidate(session, item, listing) for listing in candidates]
    scored.sort(key=lambda x: x.score, reverse=True)

    best = scored[0]
    second_score = scored[1].score if len(scored) > 1 else -999.0
    gap = best.score - second_score

    auto_resolve = False
    if best.score >= 90:
        auto_resolve = True
    elif best.score >= 75 and gap >= 10:
        auto_resolve = True
    elif best.score >= 65 and gap >= 18:
        auto_resolve = True

    if not auto_resolve:
        link.contact_id = contact.id if contact else None
        link.match_status = "ambiguous" if best.score >= 55 else "unresolved"
        link.match_strategy = "candidate_scoring"
        reason = derive_unresolved_reason(
            session,
            item,
            f"Mejor score={best.score:.1f}; gap={gap:.1f}; razones={', '.join(best.reasons[:4])}",
            best_score=best.score,
        )
        link.match_note = (
            f"reason={reason} | "
            f"score={best.score:.1f} | gap={gap:.1f} | "
            f"razones={', '.join(best.reasons[:4])}"
        )
        link.match_score = best.score
        return link.match_status, False, "candidate_scoring", best.score

    listing = best.listing
    apply_item_to_listing(item, listing, contact)
    market_event, created = create_or_update_market_event(session, item, listing)

    link.listing_id = listing.id
    link.asset_id = listing.asset_id
    link.contact_id = contact.id if contact else listing.contact_id
    link.market_event_id = market_event.id
    link.match_status = "resolved"
    link.match_strategy = build_match_strategy(best.reasons)
    link.match_note = f"reason=resolved | razones={', '.join(best.reasons[:5])}"
    link.match_score = best.score

    return "resolved", created, link.match_strategy, best.score


def reconcile_casafari_raw_items(
    session: Session,
    source_uids: list[str] | None = None,
    only_unresolved: bool = True,
    limit: int | None = 2000,
) -> dict[str, int]:
    stats = {
        "raw_items_processed": 0,
        "raw_items_resolved": 0,
        "raw_items_ambiguous": 0,
        "raw_items_unresolved": 0,
        "market_events_created": 0,
    }

    stmt = select(RawHistoryItem).order_by(RawHistoryItem.event_datetime.desc(), RawHistoryItem.id.desc())

    if source_uids:
        stmt = stmt.where(RawHistoryItem.source_uid.in_(source_uids))

    if only_unresolved:
        stmt = (
            stmt.outerjoin(CasafariEventLink, CasafariEventLink.raw_history_item_id == RawHistoryItem.id)
            .where(
                or_(
                    CasafariEventLink.id.is_(None),
                    CasafariEventLink.match_status.in_(["pending", "unresolved", "ambiguous"]),
                )
            )
        )

    if limit is not None:
        stmt = stmt.limit(limit)

    items = list(session.scalars(stmt).all())

    for item in items:
        stats["raw_items_processed"] += 1
        status, event_created, _strategy, _score = resolve_raw_item(session, item)

        if status == "resolved":
            stats["raw_items_resolved"] += 1
        elif status == "ambiguous":
            stats["raw_items_ambiguous"] += 1
        else:
            stats["raw_items_unresolved"] += 1

        if event_created:
            stats["market_events_created"] += 1

    return stats