from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session, joinedload

from core.identity.asset_matcher import area_is_close
from core.normalization.addresses import normalize_address_key
from core.normalization.phones import normalize_phone
from core.normalization.property_types import normalize_property_type
from db.models.asset import Asset
from db.models.listing import Listing
from db.models.listing_snapshot import ListingSnapshot


def get_csv_snapshot_window(session: Session) -> tuple[datetime | None, datetime | None]:
    min_dt = session.scalar(select(func.min(ListingSnapshot.snapshot_datetime)))
    max_dt = session.scalar(select(func.max(ListingSnapshot.snapshot_datetime)))
    return min_dt, max_dt


def is_inside_csv_window(session: Session, dt: datetime | None) -> bool:
    if dt is None:
        return False

    min_dt, max_dt = get_csv_snapshot_window(session)
    if min_dt is None or max_dt is None:
        return False

    day = dt.date()
    return min_dt.date() <= day <= max_dt.date()


def price_is_close(a: float | None, b: float | None) -> bool:
    if a is None or b is None:
        return False

    if a == 0 or b == 0:
        return False

    diff_ratio = abs(a - b) / max(a, b)
    return diff_ratio <= 0.15


def find_unique_listing_in_csv_window(
    session: Session,
    alert_datetime: datetime | None,
    source_portal: str | None,
    address_raw: str | None,
    property_type_raw: str | None,
    area_m2: float | None,
    price_eur: float | None,
    phone_raw: str | None,
) -> Listing | None:
    if not is_inside_csv_window(session, alert_datetime):
        return None

    address_norm = normalize_address_key(address_raw)
    if not address_norm or not source_portal:
        return None

    target_phone = normalize_phone(phone_raw)
    target_family, target_detail = normalize_property_type(property_type_raw)

    candidates = list(
        session.scalars(
            select(Listing)
            .join(Asset, Listing.asset_id == Asset.id)
            .options(
                joinedload(Listing.asset),
                joinedload(Listing.contact),
            )
            .where(
                Listing.source_portal == source_portal,
                Asset.address_norm == address_norm,
            )
        ).all()
    )

    if not candidates:
        return None

    scored: list[tuple[int, Listing]] = []

    for listing in candidates:
        score = 40  # portal + address core exacta por filtro

        asset = listing.asset
        if asset is not None:
            if target_family and asset.asset_type_family == target_family:
                score += 15
            if target_detail and asset.asset_type_detail == target_detail:
                score += 10

        listing_area = listing.area_m2 or (asset.area_m2 if asset else None)
        if area_is_close(listing_area, area_m2):
            score += 20

        if price_is_close(listing.price_eur, price_eur):
            score += 10

        if target_phone and listing.contact and listing.contact.phone_norm == target_phone:
            score += 25

        scored.append((score, listing))

    scored.sort(key=lambda x: x[0], reverse=True)

    best_score, best_listing = scored[0]
    second_score = scored[1][0] if len(scored) > 1 else -999

    # Solo aceptar si el candidato es realmente claro
    if best_score >= 70 and (best_score - second_score) >= 15:
        return best_listing

    return None