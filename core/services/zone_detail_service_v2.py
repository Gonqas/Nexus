from collections import Counter

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from core.features.zone_features import infer_zone_label, infer_zone_label_for_asset, infer_zone_label_for_listing
from core.services.zone_intelligence_service_v2 import get_zone_intelligence_v2
from db.models.asset import Asset
from db.models.listing import Listing
from db.models.market_event import MarketEvent


def get_zone_detail_v2(session: Session, zone_label: str, window_days: int = 14) -> dict:
    rows = get_zone_intelligence_v2(session, window_days=window_days)
    base_row = next((row for row in rows if row["zone_label"] == zone_label), None)

    listings = list(
        session.scalars(
            select(Listing).options(
                joinedload(Listing.asset).joinedload(Asset.building),
                joinedload(Listing.contact),
            )
        ).all()
    )

    events = list(
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

    zone_listings = [
        listing
        for listing in listings
        if infer_zone_label_for_listing(listing) == zone_label
    ]

    zone_events = []
    for event in events:
        event_asset = event.asset or (event.listing.asset if event.listing and event.listing.asset else None)
        zone = infer_zone_label_for_asset(event_asset) if event_asset else infer_zone_label(None)
        if zone == zone_label:
            zone_events.append(event)

    portal_counter = Counter()
    type_counter = Counter()
    phone_counter = Counter()

    for listing in zone_listings:
        if listing.source_portal:
            portal_counter[listing.source_portal] += 1

        asset_type = None
        if listing.asset and listing.asset.asset_type_detail:
            asset_type = listing.asset.asset_type_detail
        elif listing.asset and listing.asset.asset_type_family:
            asset_type = listing.asset.asset_type_family
        if asset_type:
            type_counter[asset_type] += 1

        if listing.contact and listing.contact.phone_raw:
            phone_counter[listing.contact.phone_raw] += 1

    recent_events = sorted(
        zone_events,
        key=lambda e: e.event_datetime or e.created_at,
        reverse=True,
    )[:20]

    detail = dict(base_row) if base_row else {"zone_label": zone_label}
    detail.update(
        {
            "top_portals": portal_counter.most_common(10),
            "top_types": type_counter.most_common(10),
            "top_phones": phone_counter.most_common(10),
            "recent_events": recent_events,
        }
    )
    return detail