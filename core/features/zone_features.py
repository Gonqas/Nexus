from __future__ import annotations

from collections import Counter, defaultdict
from statistics import mean

from sqlalchemy import func, select
from sqlalchemy.orm import Session, joinedload

from core.features.location_labels import canonical_zone_label
from core.normalization.addresses import extract_address_core
from core.normalization.text import normalize_text
from db.models.asset import Asset
from db.models.listing import Listing
from db.models.listing_snapshot import ListingSnapshot


def infer_zone_label(address: str | None) -> str:
    text = normalize_text(address)
    if not text:
        return "Sin zona"

    parts = [part.strip() for part in text.split(",") if part.strip()]
    if not parts:
        return "Sin zona"

    for part in reversed(parts):
        if any(ch.isdigit() for ch in part):
            continue
        if len(part) < 3:
            continue
        label = canonical_zone_label(part)
        if label:
            return label

    core = extract_address_core(text)
    return canonical_zone_label(core) or "Sin zona"


def infer_zone_label_from_geo(
    neighborhood: str | None,
    district: str | None,
    address: str | None = None,
) -> str:
    neighborhood_label = canonical_zone_label(neighborhood)
    if neighborhood_label:
        return neighborhood_label

    district_label = canonical_zone_label(district)
    if district_label:
        return district_label

    return infer_zone_label(address)


def infer_zone_label_for_asset(asset: Asset | None) -> str:
    if asset is None:
        return "Sin zona"

    building = getattr(asset, "building", None)

    return infer_zone_label_from_geo(
        neighborhood=asset.neighborhood or (building.neighborhood if building else None),
        district=asset.district or (building.district if building else None),
        address=asset.address_raw
        or (building.address_base if building else None)
        or asset.address_norm,
    )


def infer_zone_label_for_listing(listing: Listing | None) -> str:
    if listing is None:
        return "Sin zona"

    if listing.asset is not None:
        return infer_zone_label_for_asset(listing.asset)

    return infer_zone_label(None)


def _get_csv_window(session: Session):
    min_dt = session.scalar(select(func.min(ListingSnapshot.snapshot_datetime)))
    max_dt = session.scalar(select(func.max(ListingSnapshot.snapshot_datetime)))
    return min_dt, max_dt


def build_zone_feature_rows(session: Session) -> list[dict]:
    assets = list(
        session.scalars(
            select(Asset).options(joinedload(Asset.building), joinedload(Asset.listings))
        ).unique().all()
    )

    listings = list(
        session.scalars(
            select(Listing).options(joinedload(Listing.asset), joinedload(Listing.contact))
        ).all()
    )

    zone_data: dict[str, dict] = defaultdict(
        lambda: {
            "asset_ids": set(),
            "listing_ids": set(),
            "contact_ids": set(),
            "prices": [],
            "prices_m2": [],
            "asset_types": set(),
            "listing_portals": Counter(),
            "geo_neighborhood_assets": 0,
            "geo_district_assets": 0,
            "geo_point_assets": 0,
        }
    )

    for asset in assets:
        zone = infer_zone_label_for_asset(asset)
        bucket = zone_data[zone]
        bucket["asset_ids"].add(asset.id)

        if asset.asset_type_detail:
            bucket["asset_types"].add(asset.asset_type_detail)
        elif asset.asset_type_family:
            bucket["asset_types"].add(asset.asset_type_family)

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

        if listing.contact_id:
            bucket["contact_ids"].add(listing.contact_id)

        if listing.price_eur is not None:
            bucket["prices"].append(listing.price_eur)

        if listing.price_per_m2 is not None:
            bucket["prices_m2"].append(listing.price_per_m2)

        if listing.source_portal:
            bucket["listing_portals"][listing.source_portal] += 1

        if listing.asset and listing.asset.asset_type_detail:
            bucket["asset_types"].add(listing.asset.asset_type_detail)
        elif listing.asset and listing.asset.asset_type_family:
            bucket["asset_types"].add(listing.asset.asset_type_family)

    rows: list[dict] = []

    for zone, bucket in zone_data.items():
        assets_count = len(bucket["asset_ids"])
        listings_count = len(bucket["listing_ids"])
        contacts_count = len(bucket["contact_ids"])

        rows.append(
            {
                "zone_label": zone,
                "assets_count": assets_count,
                "listings_count": listings_count,
                "contacts_count": contacts_count,
                "avg_price_eur": round(mean(bucket["prices"]), 2) if bucket["prices"] else None,
                "avg_price_m2": round(mean(bucket["prices_m2"]), 2) if bucket["prices_m2"] else None,
                "asset_type_diversity": len(bucket["asset_types"]),
                "listings_per_asset": round(listings_count / assets_count, 3) if assets_count else 0.0,
                "top_portal": bucket["listing_portals"].most_common(1)[0][0]
                if bucket["listing_portals"]
                else None,
                "geo_neighborhood_assets": bucket["geo_neighborhood_assets"],
                "geo_district_assets": bucket["geo_district_assets"],
                "geo_point_assets": bucket["geo_point_assets"],
            }
        )

    return rows