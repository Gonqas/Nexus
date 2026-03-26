from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from core.features.zone_features import infer_zone_label_for_asset
from core.services.zone_intelligence_service_v2 import get_zone_intelligence_v2
from db.models.asset import Asset
from db.models.listing import Listing
from db.models.market_event import MarketEvent


def utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def ensure_utc_naive(dt):
    if dt is None:
        return None
    if getattr(dt, "tzinfo", None) is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _best_listing_for_asset(asset: Asset) -> Listing | None:
    listings = list(getattr(asset, "listings", []) or [])
    if not listings:
        return None

    active = [l for l in listings if (l.status or "").lower() in {"", "active", "disponible", "en venta"}]
    if active:
        listings = active

    def sort_key(listing: Listing):
        return (
            ensure_utc_naive(getattr(listing, "updated_at", None) or getattr(listing, "created_at", None))
            or datetime.min,
            listing.id or 0,
        )

    return sorted(listings, key=sort_key, reverse=True)[0]


def _asset_type(asset: Asset | None) -> str | None:
    if asset is None:
        return None
    return asset.asset_type_detail or asset.asset_type_family


def _zone_map(session: Session, window_days: int = 14) -> dict[str, dict]:
    rows = get_zone_intelligence_v2(session, window_days=window_days)
    return {row["zone_label"]: row for row in rows}


def _event_base_score(event_type: str | None) -> float:
    if event_type == "listing_detected":
        return 32.0
    if event_type == "price_drop":
        return 28.0
    if event_type == "price_raise":
        return 10.0
    if event_type == "reserved":
        return 22.0
    if event_type == "sold":
        return 18.0
    if event_type == "not_available":
        return 12.0
    if event_type == "expired":
        return 16.0
    return 8.0


def _recency_score(event_dt) -> float:
    dt = ensure_utc_naive(event_dt)
    if dt is None:
        return 0.0

    age_days = max((utc_now_naive() - dt).days, 0)
    if age_days <= 1:
        return 18.0
    if age_days <= 3:
        return 14.0
    if age_days <= 7:
        return 10.0
    if age_days <= 14:
        return 6.0
    if age_days <= 30:
        return 2.0
    return 0.0


def _price_signal_score(event: MarketEvent) -> float:
    event_type = event.event_type or ""
    if event_type != "price_drop":
        return 0.0

    old_price = event.price_old
    new_price = event.price_new
    if not old_price or not new_price or old_price <= 0:
        return 6.0

    drop_pct = max((old_price - new_price) / old_price, 0.0)
    if drop_pct >= 0.20:
        return 18.0
    if drop_pct >= 0.10:
        return 12.0
    if drop_pct >= 0.05:
        return 8.0
    return 4.0


def _zone_signal_score(zone_row: dict | None) -> float:
    if not zone_row:
        return 0.0

    capture = float(zone_row.get("zone_capture_score", 0.0))
    pressure = float(zone_row.get("zone_pressure_score", 0.0))
    confidence = float(zone_row.get("zone_confidence_score", 0.0))

    return (
        0.45 * (capture / 100.0) * 20.0
        + 0.35 * (pressure / 100.0) * 15.0
        + 0.20 * (confidence / 100.0) * 10.0
    )


def _geo_signal_score(asset: Asset | None) -> float:
    if asset is None:
        return 0.0

    score = 0.0
    if asset.neighborhood:
        score += 6.0
    elif asset.district:
        score += 3.0

    if asset.lat is not None and asset.lon is not None:
        score += 6.0

    return score


def _build_reason(event: MarketEvent, zone_row: dict | None, asset: Asset | None) -> str:
    bits: list[str] = []

    if event.event_type == "price_drop":
        bits.append("bajada de precio")
    elif event.event_type == "listing_detected":
        bits.append("entrada reciente")
    elif event.event_type == "reserved":
        bits.append("reservado")
    elif event.event_type == "sold":
        bits.append("vendido")
    elif event.event_type == "expired":
        bits.append("caducado")
    elif event.event_type == "not_available":
        bits.append("retirado")
    else:
        bits.append(event.event_type or "evento")

    if zone_row:
        if zone_row.get("zone_capture_score", 0) >= 65:
            bits.append("zona fuerte para captación")
        elif zone_row.get("zone_pressure_score", 0) >= 65:
            bits.append("zona con presión alta")

    if asset:
        if asset.neighborhood:
            bits.append("geo por barrio")
        elif asset.district:
            bits.append("geo por distrito")

    return " · ".join(bits)


def get_opportunity_queue_v2(
    session: Session,
    window_days: int = 14,
    limit: int = 100,
) -> list[dict[str, Any]]:
    zone_map = _zone_map(session, window_days=window_days)

    events = list(
        session.scalars(
            select(MarketEvent)
            .where(MarketEvent.source_channel == "casafari")
            .options(
                joinedload(MarketEvent.asset).joinedload(Asset.building),
                joinedload(MarketEvent.listing).joinedload(Listing.asset).joinedload(Asset.building),
                joinedload(MarketEvent.listing).joinedload(Listing.contact),
            )
        ).all()
    )

    rows: list[dict[str, Any]] = []

    for event in events:
        asset = event.asset or (event.listing.asset if event.listing and event.listing.asset else None)
        listing = event.listing or (_best_listing_for_asset(asset) if asset else None)

        zone_label = infer_zone_label_for_asset(asset) if asset else "Sin zona"
        zone_row = zone_map.get(zone_label)

        score = 0.0
        score += _event_base_score(event.event_type)
        score += _recency_score(event.event_datetime)
        score += _price_signal_score(event)
        score += _zone_signal_score(zone_row)
        score += _geo_signal_score(asset)

        row = {
            "event_id": event.id,
            "event_type": event.event_type,
            "event_datetime": event.event_datetime,
            "score": round(score, 2),
            "reason": _build_reason(event, zone_row, asset),
            "zone_label": zone_label,
            "zone_capture_score": zone_row.get("zone_capture_score") if zone_row else None,
            "zone_pressure_score": zone_row.get("zone_pressure_score") if zone_row else None,
            "zone_confidence_score": zone_row.get("zone_confidence_score") if zone_row else None,
            "zone_recommended_action": zone_row.get("recommended_action") if zone_row else None,
            "asset_id": asset.id if asset else None,
            "asset_type": _asset_type(asset),
            "asset_address": asset.address_raw if asset else None,
            "asset_neighborhood": asset.neighborhood if asset else None,
            "asset_district": asset.district if asset else None,
            "has_geo_point": bool(asset and asset.lat is not None and asset.lon is not None),
            "listing_id": listing.id if listing else None,
            "portal": listing.source_portal if listing else None,
            "listing_price_eur": listing.price_eur if listing else None,
            "listing_price_m2": listing.price_per_m2 if listing else None,
            "contact_id": listing.contact_id if listing else None,
            "contact_phone": listing.contact.phone_raw if listing and listing.contact else None,
            "price_old": event.price_old,
            "price_new": event.price_new,
        }
        rows.append(row)

    rows.sort(
        key=lambda r: (
            r["score"],
            ensure_utc_naive(r["event_datetime"]) or datetime.min,
        ),
        reverse=True,
    )

    return rows[:limit]