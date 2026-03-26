from sqlalchemy import select
from sqlalchemy.orm import Session

from core.identity.asset_matcher import area_is_close
from core.normalization.addresses import normalize_address_key
from core.normalization.property_types import normalize_property_type
from db.models.asset import Asset


def find_strict_existing_asset(
    session: Session,
    address_raw: str | None,
    property_type_raw: str | None,
    area_m2: float | None,
) -> Asset | None:
    address_norm = normalize_address_key(address_raw)
    _, asset_type_detail = normalize_property_type(property_type_raw)

    if not address_norm:
        return None

    candidates = list(
        session.scalars(
            select(Asset).where(Asset.address_norm == address_norm)
        ).all()
    )

    if not candidates:
        return None

    # 1. Dirección exacta + tipo exacto + área cercana
    for candidate in candidates:
        if asset_type_detail and candidate.asset_type_detail != asset_type_detail:
            continue
        if not area_is_close(candidate.area_m2, area_m2):
            continue
        return candidate

    # 2. Dirección exacta + tipo exacto
    for candidate in candidates:
        if asset_type_detail and candidate.asset_type_detail != asset_type_detail:
            continue
        return candidate

    # 3. Dirección exacta + área cercana
    for candidate in candidates:
        if area_is_close(candidate.area_m2, area_m2):
            return candidate

    # 4. Si solo hay un candidato en esa dirección, lo usamos
    if len(candidates) == 1:
        return candidates[0]

    return None