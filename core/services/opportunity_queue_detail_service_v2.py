from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from core.services.comparables_service import get_comparables_payload
from core.services.opportunity_queue_service_v2 import get_opportunity_queue_v2
from db.models.asset import Asset
from db.models.listing import Listing
from db.models.market_event import MarketEvent


def get_opportunity_detail_v2(session: Session, event_id: int, window_days: int = 14) -> dict:
    queue = get_opportunity_queue_v2(session, window_days=window_days, limit=500)
    row = next((r for r in queue if r["event_id"] == event_id), None)
    if row is None:
        return {"event_id": event_id, "found": False}

    event = session.scalar(
        select(MarketEvent)
        .where(MarketEvent.id == event_id)
        .options(
            joinedload(MarketEvent.asset).joinedload(Asset.building),
            joinedload(MarketEvent.listing).joinedload(Listing.asset).joinedload(Asset.building),
            joinedload(MarketEvent.listing).joinedload(Listing.contact),
        )
    )

    comparables = {}
    asset_id = row.get("asset_id")
    if asset_id is not None:
        comparables = get_comparables_payload(
            session,
            asset_id=asset_id,
            limit=5,
            strict_mode=True,
        )

    return {
        "found": True,
        "queue_row": row,
        "event": event,
        "comparables": comparables,
    }
