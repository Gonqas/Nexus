from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from core.features.microzones import infer_microzone_for_asset
from core.features.zone_features import infer_zone_label_for_asset
from core.normalization.text import normalize_text_key
from core.services.casafari_semantics_service import classify_phone_profile
from core.services.microzone_intelligence_service import get_microzone_intelligence
from core.services.predictive_signal_service import build_opportunity_prediction
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

    active = [
        listing
        for listing in listings
        if (listing.status or "").lower() in {"", "active", "disponible", "en venta"}
    ]
    if active:
        listings = active

    def sort_key(listing: Listing):
        return (
            ensure_utc_naive(
                getattr(listing, "updated_at", None) or getattr(listing, "created_at", None)
            )
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


def _microzone_map(session: Session, window_days: int = 14) -> dict[str, dict]:
    rows = get_microzone_intelligence(session, window_days=window_days)
    return {row["microzone_label"]: row for row in rows}


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
    if (event.event_type or "") != "price_drop":
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
    relative_heat = float(zone_row.get("zone_relative_heat_score", 0.0))
    transformation = float(zone_row.get("zone_transformation_signal_score", 0.0))

    return (
        0.34 * (capture / 100.0) * 20.0
        + 0.24 * (pressure / 100.0) * 15.0
        + 0.14 * (confidence / 100.0) * 10.0
        + 0.16 * (relative_heat / 100.0) * 14.0
        + 0.12 * (transformation / 100.0) * 14.0
    )


def _microzone_signal_score(microzone_row: dict | None) -> float:
    if not microzone_row:
        return 0.0

    capture = float(microzone_row.get("microzone_capture_score") or 0.0)
    concentration = float(microzone_row.get("microzone_concentration_score") or 0.0)
    confidence = float(microzone_row.get("microzone_confidence_score") or 0.0)

    return (
        0.50 * (capture / 100.0) * 12.0
        + 0.30 * (concentration / 100.0) * 10.0
        + 0.20 * (confidence / 100.0) * 8.0
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


def _priority_label(score: float) -> str:
    if score >= 62:
        return "alta"
    if score >= 50:
        return "media"
    return "seguimiento"


def _contact_group_key(contact_id: int | None, phone_raw: str | None) -> str:
    digits = "".join(ch for ch in (phone_raw or "") if ch.isdigit())
    if digits:
        return digits
    if contact_id is not None:
        return f"contact:{contact_id}"
    return "sin_contacto"


def _contact_group_label(contact_id: int | None, phone_raw: str | None) -> str:
    if phone_raw:
        return phone_raw
    if contact_id is not None:
        return f"contacto #{contact_id}"
    return "Sin contacto"


def _build_reason(
    event: MarketEvent,
    zone_row: dict | None,
    microzone_row: dict | None,
    asset: Asset | None,
    *,
    phone_profile: str,
) -> str:
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
            bits.append("zona fuerte para captacion")
        elif zone_row.get("zone_pressure_score", 0) >= 65:
            bits.append("zona con presion alta")
        if zone_row.get("zone_relative_heat_score", 0) >= 65:
            bits.append("actividad relativa alta")
        if zone_row.get("zone_transformation_signal_score", 0) >= 65:
            bits.append("senal transformadora alta")

    if microzone_row:
        if microzone_row.get("microzone_capture_score", 0) >= 65:
            bits.append("microzona caliente")
        elif microzone_row.get("microzone_concentration_score", 0) >= 65:
            bits.append("microzona concentrada")

    if phone_profile == "owner_like":
        bits.append("telefono owner_like")
    elif phone_profile == "broker_like":
        bits.append("telefono broker_like")

    if asset:
        if asset.neighborhood:
            bits.append("geo por barrio")
        elif asset.district:
            bits.append("geo por distrito")

    return " | ".join(bits)


def _score_breakdown_text(
    *,
    event_base: float,
    recency: float,
    price_signal: float,
    zone_signal: float,
    microzone_signal: float,
    geo_signal: float,
) -> str:
    return (
        f"evento {event_base:.1f} + recencia {recency:.1f} + "
        f"precio {price_signal:.1f} + zona {zone_signal:.1f} + "
        f"microzona {microzone_signal:.1f} + geo {geo_signal:.1f}"
    )


def get_opportunity_queue_v2(
    session: Session,
    window_days: int = 14,
    limit: int = 100,
) -> list[dict[str, Any]]:
    zone_map = _zone_map(session, window_days=window_days)
    microzone_map = _microzone_map(session, window_days=window_days)
    cutoff_dt = utc_now_naive() - timedelta(days=window_days)

    events = list(
        session.scalars(
            select(MarketEvent)
            .where(MarketEvent.source_channel == "casafari")
            .options(
                joinedload(MarketEvent.asset).joinedload(Asset.building),
                joinedload(MarketEvent.listing)
                .joinedload(Listing.asset)
                .joinedload(Asset.building),
                joinedload(MarketEvent.listing).joinedload(Listing.contact),
            )
        ).all()
    )

    phone_profile_cache: dict[str, dict] = {}
    rows: list[dict[str, Any]] = []

    for event in events:
        event_dt = ensure_utc_naive(event.event_datetime)
        if event_dt is None or event_dt < cutoff_dt:
            continue

        asset = event.asset or (
            event.listing.asset if event.listing and event.listing.asset else None
        )
        listing = event.listing or (_best_listing_for_asset(asset) if asset else None)

        zone_label = infer_zone_label_for_asset(asset) if asset else "Sin zona"
        zone_row = zone_map.get(zone_label)
        microzone_label = infer_microzone_for_asset(asset) if asset else None
        microzone_row = microzone_map.get(microzone_label) if microzone_label else None

        contact = listing.contact if listing and listing.contact else None
        contact_phone = contact.phone_raw if contact else None
        phone_key = contact_phone or ""
        if phone_key not in phone_profile_cache:
            phone_profile_cache[phone_key] = classify_phone_profile(session, contact_phone)
        phone_profile = phone_profile_cache[phone_key]["phone_profile"]

        price_drop_pct = None
        if (
            event.event_type == "price_drop"
            and event.price_old
            and event.price_new
            and event.price_old > 0
        ):
            price_drop_pct = max((event.price_old - event.price_new) / event.price_old, 0.0)

        event_base = _event_base_score(event.event_type)
        recency = _recency_score(event.event_datetime)
        price_signal = _price_signal_score(event)
        zone_signal = _zone_signal_score(zone_row)
        microzone_signal = _microzone_signal_score(microzone_row)
        geo_signal = _geo_signal_score(asset)
        prediction = build_opportunity_prediction(
            zone_row=zone_row,
            microzone_row=microzone_row,
            event_type=event.event_type,
            price_drop_pct=price_drop_pct,
            has_geo_point=bool(asset and asset.lat is not None and asset.lon is not None),
        )
        predictive_signal = float(prediction["predicted_opportunity_30d_score"]) * 0.12

        score = (
            event_base
            + recency
            + price_signal
            + zone_signal
            + microzone_signal
            + predictive_signal
            + geo_signal
        )

        row = {
            "event_id": event.id,
            "event_type": event.event_type,
            "event_datetime": event.event_datetime,
            "score": round(score, 2),
            "priority_label": _priority_label(score),
            "reason": _build_reason(
                event,
                zone_row,
                microzone_row,
                asset,
                phone_profile=phone_profile,
            ),
            "score_event_base": round(event_base, 2),
            "score_recency": round(recency, 2),
            "score_price_signal": round(price_signal, 2),
            "score_zone_signal": round(zone_signal, 2),
            "score_microzone_signal": round(microzone_signal, 2),
            "score_predictive_signal": round(predictive_signal, 2),
            "score_geo_signal": round(geo_signal, 2),
            "score_breakdown": _score_breakdown_text(
                event_base=event_base,
                recency=recency,
                price_signal=price_signal,
                zone_signal=zone_signal,
                microzone_signal=microzone_signal,
                geo_signal=geo_signal,
            )
            + f" + prediccion {predictive_signal:.1f}",
            "zone_label": zone_label,
            "zone_capture_score": zone_row.get("zone_capture_score") if zone_row else None,
            "zone_relative_heat_score": zone_row.get("zone_relative_heat_score")
            if zone_row
            else None,
            "zone_transformation_signal_score": zone_row.get(
                "zone_transformation_signal_score"
            )
            if zone_row
            else None,
            "zone_pressure_score": zone_row.get("zone_pressure_score") if zone_row else None,
            "zone_confidence_score": zone_row.get("zone_confidence_score")
            if zone_row
            else None,
            "zone_recommended_action": zone_row.get("recommended_action")
            if zone_row
            else None,
            "zone_population": zone_row.get("official_population") if zone_row else None,
            "zone_events_14d_per_10k_population": zone_row.get(
                "events_14d_per_10k_population"
            )
            if zone_row
            else None,
            "zone_vulnerability_index": zone_row.get("official_vulnerability_index")
            if zone_row
            else None,
            "microzone_label": microzone_label,
            "microzone_capture_score": microzone_row.get("microzone_capture_score")
            if microzone_row
            else None,
            "microzone_concentration_score": microzone_row.get(
                "microzone_concentration_score"
            )
            if microzone_row
            else None,
            "microzone_confidence_score": microzone_row.get(
                "microzone_confidence_score"
            )
            if microzone_row
            else None,
            "microzone_recommended_action": microzone_row.get("recommended_action")
            if microzone_row
            else None,
            "predicted_opportunity_30d_score": prediction["predicted_opportunity_30d_score"],
            "predicted_opportunity_30d_band": prediction["predicted_opportunity_30d_band"],
            "predicted_action_window_days": prediction["predicted_action_window_days"],
            "prediction_explanation": prediction["prediction_explanation"],
            "predicted_absorption_30d_score": prediction["zone_prediction"][
                "predicted_absorption_30d_score"
            ],
            "predicted_absorption_30d_band": prediction["zone_prediction"][
                "predicted_absorption_30d_band"
            ],
            "asset_id": asset.id if asset else None,
            "asset_type": _asset_type(asset),
            "asset_address": asset.address_raw if asset else None,
            "asset_neighborhood": asset.neighborhood if asset else None,
            "asset_district": asset.district if asset else None,
            "asset_lat": float(asset.lat) if asset and asset.lat is not None else None,
            "asset_lon": float(asset.lon) if asset and asset.lon is not None else None,
            "has_geo_point": bool(asset and asset.lat is not None and asset.lon is not None),
            "listing_id": listing.id if listing else None,
            "portal": listing.source_portal if listing else None,
            "listing_price_eur": listing.price_eur if listing else None,
            "listing_price_m2": listing.price_per_m2 if listing else None,
            "contact_id": listing.contact_id if listing else None,
            "contact_phone": contact_phone,
            "phone_profile": phone_profile,
            "contact_group_key": _contact_group_key(
                listing.contact_id if listing else None,
                contact_phone,
            ),
            "contact_group_label": _contact_group_label(
                listing.contact_id if listing else None,
                contact_phone,
            ),
            "price_old": event.price_old,
            "price_new": event.price_new,
        }
        rows.append(row)

    rows.sort(
        key=lambda row: (
            row["score"],
            ensure_utc_naive(row["event_datetime"]) or datetime.min,
        ),
        reverse=True,
    )

    return rows[:limit]


def filter_opportunity_rows(
    rows: list[dict[str, Any]],
    *,
    event_type_filter: str = "all",
    geo_filter: str = "all",
    min_score: float | None = None,
    zone_query: str | None = None,
) -> list[dict[str, Any]]:
    zone_query_key = normalize_text_key(zone_query)
    filtered: list[dict[str, Any]] = []

    for row in rows:
        if event_type_filter != "all" and row.get("event_type") != event_type_filter:
            continue

        if geo_filter == "with_geo" and not row.get("has_geo_point"):
            continue
        if geo_filter == "without_geo" and row.get("has_geo_point"):
            continue

        if min_score is not None and float(row.get("score") or 0.0) < min_score:
            continue

        if zone_query_key:
            haystack = normalize_text_key(
                " ".join(
                    str(value)
                    for value in (
                        row.get("zone_label"),
                        row.get("microzone_label"),
                        row.get("asset_address"),
                        row.get("asset_neighborhood"),
                        row.get("asset_district"),
                        row.get("portal"),
                        row.get("contact_phone"),
                    )
                    if value
                )
            )
            if zone_query_key not in haystack:
                continue

        filtered.append(row)

    return filtered


def build_opportunity_groups(
    rows: list[dict[str, Any]],
    *,
    group_by: str = "none",
    limit: int = 30,
) -> list[dict[str, Any]]:
    if group_by == "none":
        return []

    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if group_by == "zone":
            group_key = row.get("zone_label") or "Sin zona"
            group_label = group_key
        elif group_by == "contact":
            group_key = row.get("contact_group_key") or "sin_contacto"
            group_label = row.get("contact_group_label") or "Sin contacto"
        elif group_by == "event_type":
            group_key = row.get("event_type") or "unknown"
            group_label = group_key
        else:
            return []

        grouped.setdefault(group_key, []).append({**row, "_group_label": group_label})

    results: list[dict[str, Any]] = []
    for group_key, group_rows in grouped.items():
        group_rows = sorted(
            group_rows,
            key=lambda row: (
                float(row.get("score") or 0.0),
                ensure_utc_naive(row.get("event_datetime")) or datetime.min,
            ),
            reverse=True,
        )
        latest_dt = max(
            (
                ensure_utc_naive(row.get("event_datetime"))
                for row in group_rows
                if row.get("event_datetime")
            ),
            default=None,
        )
        top_score = max(float(row.get("score") or 0.0) for row in group_rows)
        avg_score = round(
            sum(float(row.get("score") or 0.0) for row in group_rows) / len(group_rows),
            2,
        )
        zones_count = len({row.get("zone_label") for row in group_rows if row.get("zone_label")})
        portals_count = len({row.get("portal") for row in group_rows if row.get("portal")})
        reasons = Counter(row.get("reason") or "" for row in group_rows)

        results.append(
            {
                "group_by": group_by,
                "group_key": group_key,
                "group_label": group_rows[0]["_group_label"],
                "events_count": len(group_rows),
                "top_score": round(top_score, 2),
                "avg_score": avg_score,
                "latest_event_datetime": latest_dt,
                "zones_count": zones_count,
                "portals_count": portals_count,
                "top_reason": reasons.most_common(1)[0][0] if reasons else None,
            }
        )

    results.sort(
        key=lambda row: (
            row["top_score"],
            row["events_count"],
            ensure_utc_naive(row["latest_event_datetime"]) or datetime.min,
        ),
        reverse=True,
    )
    return results[:limit]


def apply_group_selection(
    rows: list[dict[str, Any]],
    *,
    group_by: str = "none",
    group_key: str | None = None,
) -> list[dict[str, Any]]:
    if group_by == "none" or not group_key:
        return rows

    filtered: list[dict[str, Any]] = []
    for row in rows:
        if group_by == "zone" and (row.get("zone_label") or "Sin zona") == group_key:
            filtered.append(row)
        elif group_by == "contact" and (
            row.get("contact_group_key") or "sin_contacto"
        ) == group_key:
            filtered.append(row)
        elif group_by == "event_type" and (row.get("event_type") or "unknown") == group_key:
            filtered.append(row)

    return filtered
