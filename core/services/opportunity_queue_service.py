from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from core.features.zone_features import infer_zone_label_for_asset
from core.normalization.phones import normalize_phone
from core.services.zone_intelligence_service_v2 import get_zone_intelligence_v2
from db.models.listing import Listing
from db.models.market_event import MarketEvent


def ensure_utc_naive(dt):
    if dt is None:
        return None
    if getattr(dt, "tzinfo", None) is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def get_opportunity_queue(session: Session, days: int = 14, limit: int = 150) -> list[dict]:
    now_dt = datetime.now(timezone.utc).replace(tzinfo=None)
    from_dt = now_dt - timedelta(days=days)

    zones = get_zone_intelligence_v2(session, window_days=days)
    zone_map = {row["zone_label"]: row for row in zones}

    events = list(
        session.scalars(
            select(MarketEvent)
            .where(MarketEvent.source_channel == "casafari")
            .options(
                joinedload(MarketEvent.listing).joinedload(Listing.asset),
                joinedload(MarketEvent.listing).joinedload(Listing.contact),
                joinedload(MarketEvent.asset),
            )
        ).all()
    )

    rows: list[dict] = []

    for event in events:
        event_dt = ensure_utc_naive(event.event_datetime)
        if event_dt is None or event_dt < from_dt:
            continue

        listing = event.listing
        asset = event.asset or (listing.asset if listing else None)
        contact = listing.contact if listing else None

        zone_label = infer_zone_label_for_asset(asset) if asset else "Sin zona"
        zone = zone_map.get(zone_label)
        if zone is None:
            continue

        event_bonus = 0
        action = "Revisar"
        reason = ""

        if event.event_type == "listing_detected":
            event_bonus = 22
            action = "Captación rápida"
            reason = "Entrada nueva en zona priorizada"
        elif event.event_type == "price_drop":
            event_bonus = 18
            action = "Negociación / captación"
            reason = "Bajada de precio reciente"
        elif event.event_type in ("reserved", "sold", "not_available"):
            event_bonus = 10
            action = "Seguir absorción"
            reason = "Señal de salida del mercado"
        else:
            event_bonus = 6
            action = "Vigilar"
            reason = "Evento reciente"

        phone_norm = normalize_phone(contact.phone_raw if contact else None)
        if phone_norm and zone["owner_like_phone_count"] > 0:
            event_bonus += 4

        priority_score = (
            0.55 * zone["zone_capture_score"]
            + 0.20 * zone["zone_pressure_score"]
            + 0.15 * zone["zone_confidence_score"]
            + event_bonus
        )

        rows.append(
            {
                "event_datetime": event_dt,
                "zone_label": zone_label,
                "event_type": event.event_type,
                "price_new": event.price_new,
                "portal": listing.source_portal if listing else None,
                "contact_phone": contact.phone_raw if contact else None,
                "listing_label": asset.address_raw if asset and asset.address_raw else (
                    listing.listing_url if listing and listing.listing_url else "-"
                ),
                "zone_capture_score": zone["zone_capture_score"],
                "zone_pressure_score": zone["zone_pressure_score"],
                "zone_confidence_score": zone["zone_confidence_score"],
                "priority_score": round(priority_score, 1),
                "action": action,
                "reason": reason,
            }
        )

    rows.sort(
        key=lambda r: (r["priority_score"], r["event_datetime"]),
        reverse=True,
    )
    return rows[:limit]