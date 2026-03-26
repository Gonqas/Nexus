from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from statistics import mean

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from core.features.microzones import (
    infer_microzone_for_asset,
    microzone_cell_code,
    microzone_centroid,
)
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


def _normalize(values: list[float]) -> list[float]:
    if not values:
        return []

    min_v = min(values)
    max_v = max(values)
    if max_v == min_v:
        return [0.5 for _ in values]
    return [(value - min_v) / (max_v - min_v) for value in values]


def _safe_ratio(a: float, b: float) -> float:
    if not b:
        return 0.0
    return a / b


def _recommended_action(score: float, concentration: float, confidence: float) -> str:
    if confidence < 40:
        return "Validar antes de actuar"
    if score >= 70 and concentration >= 60:
        return "Ir al punto caliente"
    if score >= 58:
        return "Microzona prioritaria"
    if score >= 45:
        return "Seguir de cerca"
    return "Baja prioridad"


def _summary_text(row: dict) -> str:
    bits: list[str] = []

    if row["microzone_capture_score"] >= 70:
        bits.append("punto caliente micro")
    elif row["microzone_capture_score"] >= 55:
        bits.append("microzona activa")

    if row["microzone_concentration_score"] >= 65:
        bits.append("concentracion superior al peso del barrio")

    if row["events_14d"] > 0:
        bits.append(f"{row['events_14d']} eventos/14d")
    if row["price_drop_count"] > 0:
        bits.append(f"{row['price_drop_count']} bajadas")
    if row.get("parent_zone_label"):
        bits.append(f"base {row['parent_zone_label']}")

    return ". ".join(bits).capitalize() + "." if bits else "Sin lectura micro suficiente."


def get_microzone_intelligence(
    session: Session,
    *,
    window_days: int = 14,
    limit: int | None = None,
) -> list[dict]:
    now_dt = utc_now_naive()
    from_dt = now_dt - timedelta(days=window_days)

    assets = list(
        session.scalars(
            select(Asset).options(joinedload(Asset.building), joinedload(Asset.listings))
        ).unique().all()
    )

    listings = list(
        session.scalars(
            select(Listing).options(joinedload(Listing.asset).joinedload(Asset.building))
        ).all()
    )

    events = list(
        session.scalars(
            select(MarketEvent)
            .where(MarketEvent.source_channel == "casafari")
            .options(
                joinedload(MarketEvent.asset).joinedload(Asset.building),
                joinedload(MarketEvent.listing).joinedload(Listing.asset).joinedload(Asset.building),
            )
        ).all()
    )

    parent_zone_map = {
        row["zone_label"]: row for row in get_zone_intelligence_v2(session, window_days=window_days)
    }

    bucket_map: dict[str, dict] = defaultdict(
        lambda: {
            "parent_zone_label": None,
            "cell_code": None,
            "asset_ids": set(),
            "listing_ids": set(),
            "active_listing_ids": set(),
            "prices_m2": [],
            "events_14d": 0,
            "listing_detected_count": 0,
            "price_drop_count": 0,
            "absorption_count": 0,
            "lats": [],
            "lons": [],
        }
    )

    for asset in assets:
        microzone_label = infer_microzone_for_asset(asset)
        if not microzone_label:
            continue

        parent_zone_label = infer_zone_label_for_asset(asset)
        bucket = bucket_map[microzone_label]
        bucket["parent_zone_label"] = parent_zone_label
        bucket["cell_code"] = microzone_cell_code(asset.lat, asset.lon)
        bucket["asset_ids"].add(asset.id)
        bucket["lats"].append(float(asset.lat))
        bucket["lons"].append(float(asset.lon))

    for listing in listings:
        asset = listing.asset
        microzone_label = infer_microzone_for_asset(asset)
        if not microzone_label:
            continue

        bucket = bucket_map[microzone_label]
        bucket["listing_ids"].add(listing.id)
        if listing.status in (None, "", "active", "Disponible", "En venta"):
            bucket["active_listing_ids"].add(listing.id)
        if listing.price_per_m2 is not None:
            bucket["prices_m2"].append(float(listing.price_per_m2))

    for event in events:
        asset = event.asset or (event.listing.asset if event.listing and event.listing.asset else None)
        microzone_label = infer_microzone_for_asset(asset)
        if not microzone_label:
            continue

        event_dt = ensure_utc_naive(event.event_datetime)
        if event_dt is None or event_dt < from_dt:
            continue

        bucket = bucket_map[microzone_label]
        bucket["events_14d"] += 1

        event_type = event.event_type or ""
        if event_type == "listing_detected":
            bucket["listing_detected_count"] += 1
        elif event_type == "price_drop":
            bucket["price_drop_count"] += 1
        elif event_type in {"reserved", "sold", "not_available", "expired"}:
            bucket["absorption_count"] += 1

    rows: list[dict] = []
    for label, bucket in bucket_map.items():
        assets_count = len(bucket["asset_ids"])
        if assets_count <= 0:
            continue

        parent_zone_label = bucket["parent_zone_label"] or "Sin zona"
        parent_row = parent_zone_map.get(parent_zone_label) or {}
        parent_assets = float(parent_row.get("assets_count") or 0.0)
        parent_events = float(parent_row.get("events_14d") or 0.0)

        centroid = microzone_centroid(
            mean(bucket["lats"]) if bucket["lats"] else None,
            mean(bucket["lons"]) if bucket["lons"] else None,
        )

        row = {
            "microzone_label": label,
            "zone_label": label,
            "parent_zone_label": parent_zone_label,
            "cell_code": bucket["cell_code"],
            "assets_count": assets_count,
            "listings_count": len(bucket["listing_ids"]),
            "active_listings_count": len(bucket["active_listing_ids"]),
            "events_14d": bucket["events_14d"],
            "listing_detected_count": bucket["listing_detected_count"],
            "price_drop_count": bucket["price_drop_count"],
            "absorption_count": bucket["absorption_count"],
            "avg_price_m2": round(mean(bucket["prices_m2"]), 2) if bucket["prices_m2"] else None,
            "centroid_lat": centroid[0] if centroid else None,
            "centroid_lon": centroid[1] if centroid else None,
            "microzone_assets_share_in_parent": round(
                _safe_ratio(float(assets_count), parent_assets), 4
            ),
            "microzone_events_share_in_parent": round(
                _safe_ratio(float(bucket["events_14d"]), parent_events), 4
            ),
            "events_per_asset": round(
                _safe_ratio(float(bucket["events_14d"]), float(assets_count)), 4
            ),
            "active_listings_per_asset": round(
                _safe_ratio(float(len(bucket["active_listing_ids"])), float(assets_count)), 4
            ),
            "price_drops_per_asset": round(
                _safe_ratio(float(bucket["price_drop_count"]), float(assets_count)), 4
            ),
            "absorption_per_asset": round(
                _safe_ratio(float(bucket["absorption_count"]), float(assets_count)), 4
            ),
            "parent_zone_capture_score": parent_row.get("zone_capture_score"),
            "parent_zone_relative_heat_score": parent_row.get("zone_relative_heat_score"),
            "parent_zone_confidence_score": parent_row.get("zone_confidence_score"),
            "parent_zone_transformation_signal_score": parent_row.get(
                "zone_transformation_signal_score"
            ),
            "parent_zone_recommended_action": parent_row.get("recommended_action"),
        }
        rows.append(row)

    if not rows:
        return []

    events_per_asset = [float(row["events_per_asset"]) for row in rows]
    listings_per_asset = [float(row["active_listings_per_asset"]) for row in rows]
    drops_per_asset = [float(row["price_drops_per_asset"]) for row in rows]
    absorption_per_asset = [float(row["absorption_per_asset"]) for row in rows]
    concentration_raw = [
        _safe_ratio(
            float(row["microzone_events_share_in_parent"]),
            max(float(row["microzone_assets_share_in_parent"]), 0.0001),
        )
        for row in rows
    ]
    assets_counts = [float(row["assets_count"]) for row in rows]

    norm_events_per_asset = _normalize(events_per_asset)
    norm_listings_per_asset = _normalize(listings_per_asset)
    norm_drops_per_asset = _normalize(drops_per_asset)
    norm_absorption_per_asset = _normalize(absorption_per_asset)
    norm_concentration = _normalize(concentration_raw)
    norm_assets = _normalize(assets_counts)

    result: list[dict] = []
    for idx, row in enumerate(rows):
        parent_capture = float(row.get("parent_zone_capture_score") or 0.0) / 100.0
        parent_relative = float(row.get("parent_zone_relative_heat_score") or 0.0) / 100.0
        parent_conf = float(row.get("parent_zone_confidence_score") or 0.0) / 100.0
        parent_transform = (
            float(row.get("parent_zone_transformation_signal_score") or 0.0) / 100.0
        )

        local_intensity = (
            0.36 * norm_events_per_asset[idx]
            + 0.24 * norm_listings_per_asset[idx]
            + 0.22 * norm_drops_per_asset[idx]
            + 0.18 * norm_absorption_per_asset[idx]
        )

        parent_support = (
            0.34 * parent_capture
            + 0.28 * parent_relative
            + 0.22 * parent_conf
            + 0.16 * parent_transform
        )

        confidence = (
            0.42 * norm_assets[idx]
            + 0.38 * parent_conf
            + 0.20 * min(float(row["assets_count"]) / 4.0, 1.0)
        )

        capture = (
            0.42 * local_intensity
            + 0.30 * norm_concentration[idx]
            + 0.18 * parent_support
            + 0.10 * confidence
        )

        enriched = dict(row)
        enriched["microzone_concentration_score"] = round(norm_concentration[idx] * 100, 1)
        enriched["microzone_local_intensity_score"] = round(local_intensity * 100, 1)
        enriched["microzone_capture_score"] = round(capture * 100, 1)
        enriched["microzone_confidence_score"] = round(confidence * 100, 1)
        enriched["recommended_action"] = _recommended_action(
            enriched["microzone_capture_score"],
            enriched["microzone_concentration_score"],
            enriched["microzone_confidence_score"],
        )
        enriched["executive_summary"] = _summary_text(enriched)
        result.append(enriched)

    result.sort(
        key=lambda row: (
            row["microzone_capture_score"],
            row["microzone_concentration_score"],
            row["microzone_confidence_score"],
            row["events_14d"],
        ),
        reverse=True,
    )

    if limit is not None:
        return result[:limit]
    return result
