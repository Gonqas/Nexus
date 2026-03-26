from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from statistics import mean

from sqlalchemy import desc, select
from sqlalchemy.orm import Session, joinedload

from core.features.zone_features import (
    infer_zone_label,
    infer_zone_label_for_asset,
    infer_zone_label_for_listing,
)
from core.normalization.phones import normalize_phone
from core.services.external_zone_context_service import get_zone_external_context
from db.models.asset import Asset
from db.models.ingestion_run import IngestionRun
from db.models.listing import Listing
from db.models.market_event import MarketEvent
from db.models.raw_history_item import RawHistoryItem


def ensure_utc_naive(dt):
    if dt is None:
        return None
    if getattr(dt, "tzinfo", None) is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def safe_rate(count: float, base: float, multiplier: float) -> float:
    if not base:
        return 0.0
    return round((count / base) * multiplier, 4)


def get_last_csv_success_dt(session: Session):
    run = session.scalar(
        select(IngestionRun)
        .where(
            IngestionRun.source_type == "csv",
            IngestionRun.status == "success",
        )
        .order_by(desc(IngestionRun.finished_at), desc(IngestionRun.id))
        .limit(1)
    )
    if run is None:
        return None
    return ensure_utc_naive(run.finished_at or run.started_at or run.created_at)


def get_csv_freshness_days(session: Session) -> int | None:
    last_dt = get_last_csv_success_dt(session)
    if last_dt is None:
        return None
    return max((utc_now_naive() - last_dt).days, 0)


def build_phone_profile_map(listings: list[Listing]) -> dict[str, dict]:
    bucket = defaultdict(lambda: {"listing_ids": set(), "asset_ids": set(), "portals": set()})

    for listing in listings:
        if not listing.contact:
            continue

        phone_norm = normalize_phone(listing.contact.phone_raw or listing.contact.phone_norm)
        if not phone_norm:
            continue

        bucket[phone_norm]["listing_ids"].add(listing.id)
        if listing.asset_id:
            bucket[phone_norm]["asset_ids"].add(listing.asset_id)
        if listing.source_portal:
            bucket[phone_norm]["portals"].add(listing.source_portal)

    profile_map: dict[str, dict] = {}
    for phone_norm, data in bucket.items():
        listing_count = len(data["listing_ids"])
        asset_count = len(data["asset_ids"])
        portal_count = len(data["portals"])

        if listing_count == 1 and asset_count <= 1 and portal_count <= 1:
            profile = "owner_like"
        elif listing_count >= 4 or asset_count >= 3 or portal_count >= 3:
            profile = "broker_like"
        else:
            profile = "unknown"

        profile_map[phone_norm] = {
            "phone_profile": profile,
            "listing_count": listing_count,
            "asset_count": asset_count,
            "portal_count": portal_count,
        }

    return profile_map


def build_zone_feature_rows_v2(session: Session, window_days: int = 14) -> list[dict]:
    now_dt = utc_now_naive()
    from_dt = now_dt - timedelta(days=window_days)
    from_dt_7 = now_dt - timedelta(days=7)
    from_dt_30 = now_dt - timedelta(days=30)

    assets = list(
        session.scalars(
            select(Asset).options(joinedload(Asset.building), joinedload(Asset.listings))
        ).unique().all()
    )

    listings = list(
        session.scalars(
            select(Listing).options(
                joinedload(Listing.asset).joinedload(Asset.building),
                joinedload(Listing.contact),
            )
        ).all()
    )

    market_events = list(
        session.scalars(
            select(MarketEvent)
            .where(MarketEvent.source_channel == "casafari")
            .options(
                joinedload(MarketEvent.listing).joinedload(Listing.asset).joinedload(Asset.building),
                joinedload(MarketEvent.listing).joinedload(Listing.contact),
                joinedload(MarketEvent.asset).joinedload(Asset.building),
            )
        ).all()
    )

    casafari_raw_items = list(
        session.scalars(
            select(RawHistoryItem).where(RawHistoryItem.source_name == "casafari_history")
        ).all()
    )

    phone_profiles = build_phone_profile_map(listings)
    csv_freshness_days = get_csv_freshness_days(session)

    zone_data: dict[str, dict] = defaultdict(
        lambda: {
            "asset_ids": set(),
            "listing_ids": set(),
            "active_listing_ids": set(),
            "contact_ids": set(),
            "phone_norms": set(),
            "owner_like_phones": set(),
            "broker_like_phones": set(),
            "portal_counter": Counter(),
            "type_counter": Counter(),
            "prices": [],
            "prices_m2": [],
            "areas_m2": [],
            "bedrooms": [],
            "bathrooms": [],
            "events_7d": 0,
            "events_14d": 0,
            "events_30d": 0,
            "listing_detected_count": 0,
            "price_drop_count": 0,
            "price_raise_count": 0,
            "reserved_count": 0,
            "sold_count": 0,
            "not_available_count": 0,
            "expired_count": 0,
            "casafari_resolved_events": 0,
            "casafari_raw_in_zone": 0,
            "geo_neighborhood_assets": 0,
            "geo_district_assets": 0,
            "geo_point_assets": 0,
        }
    )

    for asset in assets:
        zone = infer_zone_label_for_asset(asset)
        bucket = zone_data[zone]
        bucket["asset_ids"].add(asset.id)

        asset_type = asset.asset_type_detail or asset.asset_type_family
        if asset_type:
            bucket["type_counter"][asset_type] += 1

        if asset.neighborhood:
            bucket["geo_neighborhood_assets"] += 1
        elif asset.district:
            bucket["geo_district_assets"] += 1

        if asset.lat is not None and asset.lon is not None:
            bucket["geo_point_assets"] += 1

    for listing in listings:
        zone = infer_zone_label_for_listing(listing)
        bucket = zone_data[zone]

        bucket["listing_ids"].add(listing.id)
        if listing.status in (None, "", "active", "Disponible", "En venta"):
            bucket["active_listing_ids"].add(listing.id)

        if listing.contact_id:
            bucket["contact_ids"].add(listing.contact_id)

        if listing.price_eur is not None:
            bucket["prices"].append(listing.price_eur)
        if listing.price_per_m2 is not None:
            bucket["prices_m2"].append(listing.price_per_m2)
        if listing.area_m2 is not None:
            bucket["areas_m2"].append(listing.area_m2)
        if listing.bedrooms is not None:
            bucket["bedrooms"].append(listing.bedrooms)
        if listing.bathrooms is not None:
            bucket["bathrooms"].append(listing.bathrooms)

        if listing.source_portal:
            bucket["portal_counter"][listing.source_portal] += 1

        phone_norm = normalize_phone(
            listing.contact.phone_raw if listing.contact else None
        )
        if phone_norm:
            bucket["phone_norms"].add(phone_norm)
            profile = phone_profiles.get(phone_norm, {}).get("phone_profile", "unknown")
            if profile == "owner_like":
                bucket["owner_like_phones"].add(phone_norm)
            elif profile == "broker_like":
                bucket["broker_like_phones"].add(phone_norm)

    for event in market_events:
        event_asset = event.asset or (event.listing.asset if event.listing and event.listing.asset else None)
        zone = infer_zone_label_for_asset(event_asset) if event_asset else infer_zone_label(None)
        bucket = zone_data[zone]
        bucket["casafari_resolved_events"] += 1

        event_dt = ensure_utc_naive(event.event_datetime)
        if event_dt is None:
            continue

        if event_dt >= from_dt_30:
            bucket["events_30d"] += 1
        if event_dt >= from_dt:
            bucket["events_14d"] += 1
        if event_dt >= from_dt_7:
            bucket["events_7d"] += 1

        if event_dt >= from_dt:
            event_type = event.event_type or "history_item"
            if event_type == "listing_detected":
                bucket["listing_detected_count"] += 1
            elif event_type == "price_drop":
                bucket["price_drop_count"] += 1
            elif event_type == "price_raise":
                bucket["price_raise_count"] += 1
            elif event_type == "reserved":
                bucket["reserved_count"] += 1
            elif event_type == "sold":
                bucket["sold_count"] += 1
            elif event_type == "not_available":
                bucket["not_available_count"] += 1
            elif event_type == "expired":
                bucket["expired_count"] += 1

    for raw_item in casafari_raw_items:
        zone = infer_zone_label(raw_item.address_raw)
        zone_data[zone]["casafari_raw_in_zone"] += 1

    rows: list[dict] = []
    for zone_label, bucket in zone_data.items():
        assets_count = len(bucket["asset_ids"])
        listings_count = len(bucket["listing_ids"])
        active_listings_count = len(bucket["active_listing_ids"])
        unique_phone_count = len(bucket["phone_norms"])
        owner_like_phone_count = len(bucket["owner_like_phones"])
        broker_like_phone_count = len(bucket["broker_like_phones"])

        absorption_count = (
            bucket["reserved_count"]
            + bucket["sold_count"]
            + bucket["not_available_count"]
            + bucket["expired_count"]
        )

        raw_count = bucket["casafari_raw_in_zone"]
        resolved_count = bucket["casafari_resolved_events"]
        unresolved_estimated = max(raw_count - resolved_count, 0)

        rows.append(
            {
                "zone_label": zone_label,
                "window_days": window_days,
                "csv_freshness_days": csv_freshness_days,
                "assets_count": assets_count,
                "listings_count": listings_count,
                "active_listings_count": active_listings_count,
                "contacts_count": len(bucket["contact_ids"]),
                "unique_phone_count": unique_phone_count,
                "owner_like_phone_count": owner_like_phone_count,
                "broker_like_phone_count": broker_like_phone_count,
                "broker_phone_share": round(
                    broker_like_phone_count / unique_phone_count, 4
                ) if unique_phone_count else 0.0,
                "owner_phone_share": round(
                    owner_like_phone_count / unique_phone_count, 4
                ) if unique_phone_count else 0.0,
                "avg_price_eur": round(mean(bucket["prices"]), 2) if bucket["prices"] else None,
                "avg_price_m2": round(mean(bucket["prices_m2"]), 2) if bucket["prices_m2"] else None,
                "avg_area_m2": round(mean(bucket["areas_m2"]), 2) if bucket["areas_m2"] else None,
                "avg_bedrooms": round(mean(bucket["bedrooms"]), 2) if bucket["bedrooms"] else None,
                "avg_bathrooms": round(mean(bucket["bathrooms"]), 2) if bucket["bathrooms"] else None,
                "asset_type_diversity": len(bucket["type_counter"]),
                "portal_diversity": len(bucket["portal_counter"]),
                "top_portal": bucket["portal_counter"].most_common(1)[0][0]
                if bucket["portal_counter"] else None,
                "listings_per_asset": round(listings_count / assets_count, 3) if assets_count else 0.0,
                "events_7d": bucket["events_7d"],
                "events_14d": bucket["events_14d"],
                "events_30d": bucket["events_30d"],
                "listing_detected_count": bucket["listing_detected_count"],
                "price_drop_count": bucket["price_drop_count"],
                "price_raise_count": bucket["price_raise_count"],
                "reserved_count": bucket["reserved_count"],
                "sold_count": bucket["sold_count"],
                "not_available_count": bucket["not_available_count"],
                "expired_count": bucket["expired_count"],
                "absorption_count": absorption_count,
                "net_new_supply": bucket["listing_detected_count"] - absorption_count,
                "casafari_resolved_events": resolved_count,
                "casafari_raw_in_zone": raw_count,
                "casafari_unresolved_estimated": unresolved_estimated,
                "resolved_ratio": round(resolved_count / raw_count, 4) if raw_count else 0.0,
                "geo_neighborhood_assets": bucket["geo_neighborhood_assets"],
                "geo_district_assets": bucket["geo_district_assets"],
                "geo_point_assets": bucket["geo_point_assets"],
                "geo_coverage_ratio": round(
                    bucket["geo_point_assets"] / assets_count, 4
                ) if assets_count else 0.0,
            }
        )

    enriched_rows: list[dict] = []
    for row in rows:
        context = get_zone_external_context(row["zone_label"])
        population = float(context.get("population") or 0.0)

        enriched = dict(row)
        enriched["context_zone_level"] = context.get("zone_level")
        enriched["context_district_label"] = context.get("district_label")
        enriched["official_population"] = context.get("population")
        enriched["official_population_date"] = context.get("population_date")
        enriched["official_household_income_eur"] = context.get("household_income_eur")
        enriched["official_cadastral_value_mean"] = context.get("cadastral_value_mean")
        enriched["official_vulnerability_index"] = context.get("vulnerability_index")
        enriched["official_foreign_population_rate"] = context.get("foreign_population_rate")
        enriched["official_abstention_rate"] = context.get("abstention_rate")
        enriched["official_age_dependency_share"] = context.get("age_dependency_share")
        enriched["official_change_of_use_24m"] = context.get("change_of_use_24m")
        enriched["official_new_dwelling_24m"] = context.get("new_dwelling_24m")
        enriched["official_urban_licenses_24m"] = context.get("urban_licenses_24m")
        enriched["official_urban_inspections_24m"] = (
            context.get("urban_inspections_24m") or context.get("district_urban_inspections_24m")
        )
        enriched["official_discipline_inspections_24m"] = (
            context.get("discipline_inspections_24m") or context.get("district_discipline_inspections_24m")
        )
        enriched["official_locales_total"] = context.get("locales_total")
        enriched["official_locales_open"] = context.get("locales_open")
        enriched["official_locales_closed"] = context.get("locales_closed")
        enriched["official_vut_units"] = context.get("vut_units") or context.get("district_vut_units")
        enriched["assets_per_1k_population"] = safe_rate(assets_count, population, 1000.0)
        enriched["active_listings_per_1k_population"] = safe_rate(
            row["active_listings_count"], population, 1000.0
        )
        enriched["events_14d_per_10k_population"] = safe_rate(
            row["events_14d"], population, 10000.0
        )
        enriched["listing_detected_per_10k_population"] = safe_rate(
            row["listing_detected_count"], population, 10000.0
        )
        enriched["price_drop_per_10k_population"] = safe_rate(
            row["price_drop_count"], population, 10000.0
        )
        enriched["absorption_per_10k_population"] = safe_rate(
            row["absorption_count"], population, 10000.0
        )
        enriched["casafari_raw_per_10k_population"] = safe_rate(
            row["casafari_raw_in_zone"], population, 10000.0
        )
        enriched["change_of_use_per_10k_population"] = context.get("change_of_use_per_10k_population")
        enriched["urban_inspections_per_10k_population"] = context.get("urban_inspections_per_10k_population")
        enriched["closed_locales_per_1k_population"] = context.get("closed_locales_per_1k_population")
        enriched["vut_units_per_1k_population"] = context.get("vut_units_per_1k_population")
        enriched_rows.append(enriched)

    return enriched_rows
