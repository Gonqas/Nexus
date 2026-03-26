from __future__ import annotations

from dataclasses import dataclass
from math import asin, cos, radians, sin, sqrt
from statistics import mean
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from core.features.zone_features import infer_zone_label_for_asset
from db.models.asset import Asset
from db.models.listing import Listing


EARTH_RADIUS_KM = 6371.0088


@dataclass(slots=True)
class ComparableCandidate:
    asset: Asset
    listing: Listing | None
    distance_km: float | None
    area_delta_pct: float | None
    price_m2_delta_pct: float | None
    same_asset_type: bool
    same_neighborhood: bool
    same_district: bool
    score: float


def haversine_km(
    lat1: float | None,
    lon1: float | None,
    lat2: float | None,
    lon2: float | None,
) -> float | None:
    if None in (lat1, lon1, lat2, lon2):
        return None

    lat1_r = radians(lat1)
    lon1_r = radians(lon1)
    lat2_r = radians(lat2)
    lon2_r = radians(lon2)

    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r

    a = sin(dlat / 2) ** 2 + cos(lat1_r) * cos(lat2_r) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return EARTH_RADIUS_KM * c


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
        price = listing.price_eur if listing.price_eur is not None else 10**15
        return (price, -(listing.id or 0))

    return sorted(listings, key=sort_key)[0]


def _safe_pct_delta(base: float | None, other: float | None) -> float | None:
    if base is None or other is None or base == 0:
        return None
    return abs(other - base) / abs(base)


def _asset_type(asset: Asset | None) -> str | None:
    if asset is None:
        return None
    return asset.asset_type_detail or asset.asset_type_family


def _geo_for_asset(asset: Asset) -> dict[str, Any]:
    building = getattr(asset, "building", None)
    return {
        "lat": asset.lat if asset.lat is not None else (building.lat if building else None),
        "lon": asset.lon if asset.lon is not None else (building.lon if building else None),
        "neighborhood": asset.neighborhood or (building.neighborhood if building else None),
        "district": asset.district or (building.district if building else None),
    }


def _listing_price_m2(listing: Listing | None) -> float | None:
    if listing is None:
        return None
    if listing.price_per_m2 is not None:
        return listing.price_per_m2
    if listing.price_eur is not None and listing.area_m2:
        if listing.area_m2 != 0:
            return listing.price_eur / listing.area_m2
    return None


def _score_candidate(
    subject_asset: Asset,
    subject_listing: Listing | None,
    candidate_asset: Asset,
    candidate_listing: Listing | None,
) -> ComparableCandidate:
    subject_geo = _geo_for_asset(subject_asset)
    candidate_geo = _geo_for_asset(candidate_asset)

    distance_km = haversine_km(
        subject_geo["lat"],
        subject_geo["lon"],
        candidate_geo["lat"],
        candidate_geo["lon"],
    )

    subject_area = subject_asset.area_m2 or (subject_listing.area_m2 if subject_listing else None)
    candidate_area = candidate_asset.area_m2 or (candidate_listing.area_m2 if candidate_listing else None)
    area_delta_pct = _safe_pct_delta(subject_area, candidate_area)

    subject_price_m2 = _listing_price_m2(subject_listing)
    candidate_price_m2 = _listing_price_m2(candidate_listing)
    price_m2_delta_pct = _safe_pct_delta(subject_price_m2, candidate_price_m2)

    subject_type = _asset_type(subject_asset)
    candidate_type = _asset_type(candidate_asset)

    same_asset_type = bool(subject_type and candidate_type and subject_type == candidate_type)
    same_neighborhood = bool(
        subject_geo["neighborhood"]
        and candidate_geo["neighborhood"]
        and subject_geo["neighborhood"] == candidate_geo["neighborhood"]
    )
    same_district = bool(
        subject_geo["district"]
        and candidate_geo["district"]
        and subject_geo["district"] == candidate_geo["district"]
    )

    score = 0.0

    if distance_km is not None:
        if distance_km <= 0.25:
            score += 40
        elif distance_km <= 0.5:
            score += 34
        elif distance_km <= 1.0:
            score += 27
        elif distance_km <= 2.0:
            score += 17
        elif distance_km <= 3.0:
            score += 8
    else:
        if same_neighborhood:
            score += 22
        elif same_district:
            score += 12
        elif infer_zone_label_for_asset(subject_asset) == infer_zone_label_for_asset(candidate_asset):
            score += 7

    if same_asset_type:
        score += 30
    else:
        score -= 12

    if area_delta_pct is not None:
        if area_delta_pct <= 0.10:
            score += 22
        elif area_delta_pct <= 0.20:
            score += 15
        elif area_delta_pct <= 0.35:
            score += 8
        elif area_delta_pct <= 0.50:
            score += 2
        elif area_delta_pct > 0.80:
            score -= 18
        elif area_delta_pct > 0.60:
            score -= 10

    if price_m2_delta_pct is not None:
        if price_m2_delta_pct <= 0.10:
            score += 12
        elif price_m2_delta_pct <= 0.20:
            score += 8
        elif price_m2_delta_pct <= 0.35:
            score += 3
        elif price_m2_delta_pct > 0.50:
            score -= 6

    return ComparableCandidate(
        asset=candidate_asset,
        listing=candidate_listing,
        distance_km=distance_km,
        area_delta_pct=area_delta_pct,
        price_m2_delta_pct=price_m2_delta_pct,
        same_asset_type=same_asset_type,
        same_neighborhood=same_neighborhood,
        same_district=same_district,
        score=round(score, 3),
    )


def get_comparable_candidates(
    session: Session,
    asset_id: int,
    limit: int = 12,
    max_distance_km: float = 3.0,
    strict_mode: bool = True,
) -> list[ComparableCandidate]:
    assets = list(
        session.scalars(
            select(Asset).options(
                joinedload(Asset.building),
                joinedload(Asset.listings),
            )
        ).unique().all()
    )

    subject = next((asset for asset in assets if asset.id == asset_id), None)
    if subject is None:
        return []

    subject_type = _asset_type(subject)
    subject_listing = _best_listing_for_asset(subject)

    candidates: list[ComparableCandidate] = []

    for candidate in assets:
        if candidate.id == subject.id:
            continue

        candidate_listing = _best_listing_for_asset(candidate)
        scored = _score_candidate(subject, subject_listing, candidate, candidate_listing)

        if strict_mode:
            if subject_type and _asset_type(candidate) and _asset_type(candidate) != subject_type:
                continue
            if scored.area_delta_pct is not None and scored.area_delta_pct > 0.45:
                continue

        if scored.score <= 0:
            continue

        if scored.distance_km is not None and scored.distance_km > max_distance_km:
            continue

        if (
            scored.distance_km is None
            and not scored.same_neighborhood
            and not scored.same_district
            and infer_zone_label_for_asset(candidate) != infer_zone_label_for_asset(subject)
        ):
            continue

        candidates.append(scored)

    candidates.sort(
        key=lambda candidate: (
            -candidate.score,
            candidate.distance_km if candidate.distance_km is not None else 999999,
            candidate.area_delta_pct if candidate.area_delta_pct is not None else 999999,
        )
    )
    return candidates[:limit]


def get_comparables_payload(
    session: Session,
    asset_id: int,
    limit: int = 12,
    max_distance_km: float = 3.0,
    strict_mode: bool = True,
) -> dict[str, Any]:
    assets = list(
        session.scalars(
            select(Asset).options(
                joinedload(Asset.building),
                joinedload(Asset.listings),
            )
        ).unique().all()
    )
    subject = next((asset for asset in assets if asset.id == asset_id), None)
    if subject is None:
        return {"asset_id": asset_id, "subject": None, "comparables": [], "summary": {}}

    subject_listing = _best_listing_for_asset(subject)
    subject_geo = _geo_for_asset(subject)

    comps = get_comparable_candidates(
        session=session,
        asset_id=asset_id,
        limit=limit,
        max_distance_km=max_distance_km,
        strict_mode=strict_mode,
    )

    used_strict_mode = strict_mode
    if strict_mode and len(comps) < 3:
        comps = get_comparable_candidates(
            session=session,
            asset_id=asset_id,
            limit=limit,
            max_distance_km=max_distance_km,
            strict_mode=False,
        )
        used_strict_mode = False

    subject_payload = {
        "asset_id": subject.id,
        "zone_label": infer_zone_label_for_asset(subject),
        "asset_type": _asset_type(subject),
        "area_m2": subject.area_m2 or (subject_listing.area_m2 if subject_listing else None),
        "price_eur": subject_listing.price_eur if subject_listing else None,
        "price_m2": _listing_price_m2(subject_listing),
        "lat": subject_geo["lat"],
        "lon": subject_geo["lon"],
        "neighborhood": subject_geo["neighborhood"],
        "district": subject_geo["district"],
    }

    comparable_rows = []
    for comp in comps:
        comp_geo = _geo_for_asset(comp.asset)
        comparable_rows.append(
            {
                "asset_id": comp.asset.id,
                "listing_id": comp.listing.id if comp.listing else None,
                "zone_label": infer_zone_label_for_asset(comp.asset),
                "asset_type": _asset_type(comp.asset),
                "area_m2": comp.asset.area_m2 or (comp.listing.area_m2 if comp.listing else None),
                "price_eur": comp.listing.price_eur if comp.listing else None,
                "price_m2": _listing_price_m2(comp.listing),
                "distance_km": round(comp.distance_km, 3) if comp.distance_km is not None else None,
                "area_delta_pct": round(comp.area_delta_pct, 4) if comp.area_delta_pct is not None else None,
                "price_m2_delta_pct": round(comp.price_m2_delta_pct, 4) if comp.price_m2_delta_pct is not None else None,
                "same_asset_type": comp.same_asset_type,
                "same_neighborhood": comp.same_neighborhood,
                "same_district": comp.same_district,
                "score": comp.score,
                "lat": comp_geo["lat"],
                "lon": comp_geo["lon"],
                "neighborhood": comp_geo["neighborhood"],
                "district": comp_geo["district"],
            }
        )

    summary_price_m2 = [row["price_m2"] for row in comparable_rows if row["price_m2"] is not None]

    return {
        "asset_id": asset_id,
        "subject": subject_payload,
        "comparables": comparable_rows,
        "summary": {
            "comparables_count": len(comparable_rows),
            "avg_comparable_price_m2": round(mean(summary_price_m2), 2) if summary_price_m2 else None,
            "requested_strict_mode": strict_mode,
            "used_strict_mode": used_strict_mode,
        },
    }