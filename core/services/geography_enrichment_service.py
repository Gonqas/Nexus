from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from core.geography.madrid_street_catalog import StreetMatch
from core.normalization.addresses import (
    get_madrid_street_catalog,
    normalize_address_key,
    normalize_address_raw,
)
from db.models.asset import Asset
from db.models.building import Building


def _best_address_for_asset(asset: Asset) -> str | None:
    if asset.address_raw:
        return asset.address_raw
    if asset.building and asset.building.address_base:
        return asset.building.address_base
    return None


def _apply_match_to_building(building: Building, match: StreetMatch) -> bool:
    changed = False

    if match.district and building.district != match.district:
        building.district = match.district
        changed = True

    if match.neighborhood and building.neighborhood != match.neighborhood:
        building.neighborhood = match.neighborhood
        changed = True

    if match.lat is not None and building.lat != match.lat:
        building.lat = match.lat
        changed = True

    if match.lon is not None and building.lon != match.lon:
        building.lon = match.lon
        changed = True

    return changed


def _apply_match_to_asset(asset: Asset, match: StreetMatch) -> bool:
    changed = False

    if match.district and asset.district != match.district:
        asset.district = match.district
        changed = True

    if match.neighborhood and asset.neighborhood != match.neighborhood:
        asset.neighborhood = match.neighborhood
        changed = True

    if match.lat is not None and asset.lat != match.lat:
        asset.lat = match.lat
        changed = True

    if match.lon is not None and asset.lon != match.lon:
        asset.lon = match.lon
        changed = True

    target_confidence = _confidence_from_match(match, current=asset.data_confidence)
    if target_confidence is not None and asset.data_confidence != target_confidence:
        asset.data_confidence = target_confidence
        changed = True

    return changed


def _confidence_from_match(match: StreetMatch, current: float | None) -> float | None:
    if not match.matched:
        return current

    base = 0.72

    if match.match_type == "exact_lookup_key":
        base = 0.96 if match.neighborhood or match.lat is not None else 0.90
    elif match.match_type == "exact_name_only":
        base = 0.90 if match.neighborhood or match.lat is not None else 0.84
    elif match.match_type == "fuzzy_lookup_key":
        base = max(0.78, min(match.confidence, 0.92))
    elif match.match_type == "fuzzy_name_only":
        base = max(0.74, min(match.confidence * 0.97, 0.88))

    if current is None:
        return round(base, 4)

    return round(max(current, base), 4)


def enrich_building_geography(building: Building | None) -> bool:
    if building is None:
        return False

    catalog = get_madrid_street_catalog()
    if catalog is None:
        return False

    address_text = building.address_base
    if not address_text:
        return False

    match = catalog.resolve(address_text)
    if not match.matched:
        return False

    return _apply_match_to_building(building, match)


def enrich_asset_geography(asset: Asset | None) -> bool:
    if asset is None:
        return False

    catalog = get_madrid_street_catalog()
    if catalog is None:
        return False

    address_text = _best_address_for_asset(asset)
    if not address_text:
        return False

    match = catalog.resolve(address_text)
    changed = False

    if match.matched:
        changed = _apply_match_to_asset(asset, match)

    if asset.building is not None:
        changed = enrich_building_geography(asset.building) or changed

        if asset.district is None and asset.building.district is not None:
            asset.district = asset.building.district
            changed = True

        if asset.neighborhood is None and asset.building.neighborhood is not None:
            asset.neighborhood = asset.building.neighborhood
            changed = True

        if asset.lat is None and asset.building.lat is not None:
            asset.lat = asset.building.lat
            changed = True

        if asset.lon is None and asset.building.lon is not None:
            asset.lon = asset.building.lon
            changed = True

    return changed


def backfill_assets_geography(
    session: Session,
    limit: int | None = None,
    only_missing: bool = True,
) -> dict:
    stmt = (
        select(Asset)
        .options(joinedload(Asset.building))
        .order_by(Asset.id.asc())
    )

    if limit:
        stmt = stmt.limit(limit)

    assets = list(session.scalars(stmt).unique().all())

    scanned = 0
    changed_assets = 0

    for asset in assets:
        scanned += 1

        if only_missing:
            has_all_geo = any(
                [
                    asset.district is not None,
                    asset.neighborhood is not None,
                    asset.lat is not None,
                    asset.lon is not None,
                ]
            )
            if has_all_geo:
                continue

        if enrich_asset_geography(asset):
            changed_assets += 1

    return {
        "scanned": scanned,
        "changed_assets": changed_assets,
    }


def backfill_buildings_geography(
    session: Session,
    limit: int | None = None,
    only_missing: bool = True,
) -> dict:
    stmt = select(Building).order_by(Building.id.asc())

    if limit:
        stmt = stmt.limit(limit)

    buildings = list(session.scalars(stmt).all())

    scanned = 0
    changed_buildings = 0

    for building in buildings:
        scanned += 1

        if only_missing:
            has_all_geo = any(
                [
                    building.district is not None,
                    building.neighborhood is not None,
                    building.lat is not None,
                    building.lon is not None,
                ]
            )
            if has_all_geo:
                continue

        if enrich_building_geography(building):
            changed_buildings += 1

    return {
        "scanned": scanned,
        "changed_buildings": changed_buildings,
    }


def normalize_existing_addresses(session: Session, limit: int | None = None) -> dict:
    stmt = (
        select(Asset)
        .options(joinedload(Asset.building))
        .order_by(Asset.id.asc())
    )

    if limit:
        stmt = stmt.limit(limit)

    assets = list(session.scalars(stmt).unique().all())

    scanned = 0
    changed = 0

    for asset in assets:
        scanned += 1

        if asset.address_raw:
            normalized = normalize_address_raw(asset.address_raw)
            if normalized and normalized != asset.address_raw:
                asset.address_raw = normalized
                changed += 1

            normalized_key = normalize_address_key(asset.address_raw)
            if normalized_key and normalized_key != asset.address_norm:
                asset.address_norm = normalized_key
                changed += 1

        if asset.building and asset.building.address_base:
            normalized_base = normalize_address_raw(asset.building.address_base)
            if normalized_base and normalized_base != asset.building.address_base:
                asset.building.address_base = normalized_base
                changed += 1

    return {
        "scanned": scanned,
        "changed_rows": changed,
    }
