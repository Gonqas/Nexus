from collections import Counter
from statistics import mean

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from core.features.zone_features import infer_zone_label_for_listing
from db.models.listing import Listing


def get_zone_detail(session: Session, zone_label: str) -> dict:
    listings = list(
        session.scalars(
            select(Listing).options(
                joinedload(Listing.asset),
                joinedload(Listing.contact),
            )
        ).all()
    )

    zone_listings = [
        listing
        for listing in listings
        if infer_zone_label_for_listing(listing) == zone_label
    ]

    portal_counter = Counter()
    type_counter = Counter()

    prices = []
    prices_m2 = []

    for listing in zone_listings:
        if listing.source_portal:
            portal_counter[listing.source_portal] += 1

        if listing.asset and listing.asset.asset_type_detail:
            type_counter[listing.asset.asset_type_detail] += 1
        elif listing.asset and listing.asset.asset_type_family:
            type_counter[listing.asset.asset_type_family] += 1

        if listing.price_eur is not None:
            prices.append(listing.price_eur)

        if listing.price_per_m2 is not None:
            prices_m2.append(listing.price_per_m2)

    return {
        "zone_label": zone_label,
        "assets_count": len({listing.asset_id for listing in zone_listings if listing.asset_id}),
        "listings_count": len(zone_listings),
        "avg_price_eur": round(mean(prices), 2) if prices else None,
        "avg_price_m2": round(mean(prices_m2), 2) if prices_m2 else None,
        "top_portals": portal_counter.most_common(10),
        "top_types": type_counter.most_common(10),
    }