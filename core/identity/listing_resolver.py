from sqlalchemy import select
from sqlalchemy.orm import Session

from core.normalization.urls import normalize_url
from db.models.listing import Listing


def extract_external_id(listing_url: str | None) -> str | None:
    if not listing_url:
        return None

    listing_url = normalize_url(listing_url)
    if not listing_url:
        return None

    last_part = listing_url.rstrip("/").split("/")[-1]
    if last_part.startswith("listing-"):
        return last_part.replace("listing-", "").strip() or None

    parts = listing_url.rstrip("/").split("-")
    return parts[-1] if parts else None


def find_existing_listing(
    session: Session,
    listing_url: str | None,
    property_url: str | None,
    external_id: str | None,
    source_portal: str | None,
    asset_id: int | None,
    contact_id: int | None,
) -> Listing | None:
    listing_url = normalize_url(listing_url)
    property_url = normalize_url(property_url)

    if listing_url:
        listing = session.scalar(
            select(Listing).where(Listing.listing_url == listing_url)
        )
        if listing is not None:
            return listing

    if property_url:
        listing = session.scalar(
            select(Listing).where(Listing.property_url == property_url)
        )
        if listing is not None:
            return listing

    if external_id:
        listing = session.scalar(
            select(Listing).where(Listing.external_id == external_id)
        )
        if listing is not None:
            return listing

    if asset_id is not None and source_portal:
        stmt = select(Listing).where(
            Listing.asset_id == asset_id,
            Listing.source_portal == source_portal,
        )

        if contact_id is not None:
            stmt = stmt.where(Listing.contact_id == contact_id)

        listing = session.scalar(stmt.limit(1))
        if listing is not None:
            return listing

    return None