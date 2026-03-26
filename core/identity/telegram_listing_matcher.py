from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from core.identity.asset_matcher import area_is_close
from core.normalization.addresses import normalize_address_key
from db.models.listing import Listing


def find_listing_by_contact_portal_and_shape(
    session: Session,
    source_portal: str | None,
    contact_id: int | None,
    address_raw: str | None,
    area_m2: float | None,
) -> Listing | None:
    if not source_portal or contact_id is None:
        return None

    target_address = normalize_address_key(address_raw)

    candidates = list(
        session.scalars(
            select(Listing)
            .options(joinedload(Listing.asset))
            .where(
                Listing.source_portal == source_portal,
                Listing.contact_id == contact_id,
            )
        ).all()
    )

    best = None
    best_score = 0

    for listing in candidates:
        score = 0

        if listing.asset and listing.asset.address_norm and target_address:
            if listing.asset.address_norm == target_address:
                score += 60

        if area_is_close(listing.area_m2, area_m2):
            score += 30

        if listing.asset_id is not None:
            score += 10

        if score > best_score:
            best = listing
            best_score = score

    if best_score >= 70:
        return best

    return None