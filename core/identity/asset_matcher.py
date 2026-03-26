from rapidfuzz import fuzz
from sqlalchemy import select
from sqlalchemy.orm import Session

from db.models.asset import Asset


def area_is_close(a: float | None, b: float | None) -> bool:
    if a is None or b is None:
        return True

    tolerance = max(5.0, max(a, b) * 0.08)
    return abs(a - b) <= tolerance


def _score_candidate(
    candidate: Asset,
    address_norm: str | None,
    asset_type_detail: str | None,
    area_m2: float | None,
    building_id: int | None,
) -> float:
    score = 0.0

    if building_id and candidate.building_id == building_id:
        score += 50

    if address_norm and candidate.address_norm:
        if candidate.address_norm == address_norm:
            score += 40
        else:
            similarity = fuzz.ratio(candidate.address_norm, address_norm)
            if similarity >= 92:
                score += 20

    if asset_type_detail and candidate.asset_type_detail == asset_type_detail:
        score += 20

    if area_is_close(candidate.area_m2, area_m2):
        score += 20

    return score


def find_existing_asset(
    session: Session,
    address_norm: str | None,
    asset_type_detail: str | None,
    area_m2: float | None,
    building_id: int | None,
) -> Asset | None:
    candidates_by_id: dict[int, Asset] = {}

    if building_id is not None:
        for asset in session.scalars(
            select(Asset).where(Asset.building_id == building_id)
        ).all():
            candidates_by_id[asset.id] = asset

    if address_norm:
        for asset in session.scalars(
            select(Asset).where(Asset.address_norm == address_norm)
        ).all():
            candidates_by_id[asset.id] = asset

    # fallback fuzzy: escaneo controlado
    if not candidates_by_id and asset_type_detail:
        for asset in session.scalars(
            select(Asset).where(Asset.asset_type_detail == asset_type_detail).limit(1000)
        ).all():
            candidates_by_id[asset.id] = asset

    best_asset = None
    best_score = 0.0

    for candidate in candidates_by_id.values():
        score = _score_candidate(
            candidate=candidate,
            address_norm=address_norm,
            asset_type_detail=asset_type_detail,
            area_m2=area_m2,
            building_id=building_id,
        )
        if score > best_score:
            best_asset = candidate
            best_score = score

    if best_score >= 60:
        return best_asset

    return None