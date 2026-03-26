from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from core.config.settings import CASAFARI_SOURCE_NAME
from core.services.casafari_semantics_service import (
    classify_address_semantics,
    classify_price_semantics,
)
from core.services.zone_intelligence_service_v2 import get_zone_intelligence_v2
from db.models.asset import Asset
from db.models.building import Building
from db.models.casafari_event_link import CasafariEventLink
from db.models.contact import Contact
from db.models.listing import Listing
from db.models.market_event import MarketEvent
from db.models.raw_history_item import RawHistoryItem
from db.models.source_sync_state import SourceSyncState


def utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _safe_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)


def _recent_count(values: list[datetime | None], days: int) -> int:
    cutoff = utc_now_naive() - timedelta(days=days)
    return sum(1 for value in values if value and value >= cutoff)


def get_dashboard_stats(session: Session) -> dict[str, int | float | str | list[dict] | None]:
    assets = int(session.scalar(select(func.count(Asset.id))) or 0)
    buildings = int(session.scalar(select(func.count(Building.id))) or 0)
    contacts = int(session.scalar(select(func.count(Contact.id))) or 0)
    listings = int(session.scalar(select(func.count(Listing.id))) or 0)
    events = int(session.scalar(select(func.count(MarketEvent.id))) or 0)

    raw_items = list(
        session.scalars(
            select(RawHistoryItem).where(RawHistoryItem.source_name == CASAFARI_SOURCE_NAME)
        ).all()
    )
    casafari_raw = len(raw_items)

    total_links = int(session.scalar(select(func.count(CasafariEventLink.id))) or 0)

    casafari_resolved = int(
        session.scalar(
            select(func.count(CasafariEventLink.id)).where(
                CasafariEventLink.match_status == "resolved"
            )
        )
        or 0
    )

    casafari_ambiguous = int(
        session.scalar(
            select(func.count(CasafariEventLink.id)).where(
                CasafariEventLink.match_status == "ambiguous"
            )
        )
        or 0
    )

    casafari_unresolved = int(
        session.scalar(
            select(func.count(CasafariEventLink.id)).where(
                CasafariEventLink.match_status == "unresolved"
            )
        )
        or 0
    )

    casafari_events = int(
        session.scalar(
            select(func.count(MarketEvent.id)).where(
                MarketEvent.source_channel == "casafari"
            )
        )
        or 0
    )

    assets_with_district = int(
        session.scalar(
            select(func.count(Asset.id)).where(Asset.district.is_not(None))
        )
        or 0
    )

    assets_with_neighborhood = int(
        session.scalar(
            select(func.count(Asset.id)).where(Asset.neighborhood.is_not(None))
        )
        or 0
    )

    assets_with_geo_point = int(
        session.scalar(
            select(func.count(Asset.id)).where(
                Asset.lat.is_not(None),
                Asset.lon.is_not(None),
            )
        )
        or 0
    )

    raws_with_reliable_price = 0
    raws_with_poor_address = 0

    for item in raw_items:
        price_meta = classify_price_semantics(
            item.current_price_eur,
            item.previous_price_eur,
            item.raw_text,
        )
        if price_meta["price_confidence"] == "high":
            raws_with_reliable_price += 1

        address_meta = classify_address_semantics(item.address_raw)
        if address_meta["address_precision"] in {"zone_like", "unknown"}:
            raws_with_poor_address += 1

    casafari_event_type_rows = session.execute(
        select(MarketEvent.event_type, func.count(MarketEvent.id))
        .where(MarketEvent.source_channel == "casafari")
        .group_by(MarketEvent.event_type)
        .order_by(desc(func.count(MarketEvent.id)), MarketEvent.event_type)
    ).all()

    event_type_breakdown = [
        {
            "event_type": event_type or "unknown",
            "count": int(count or 0),
        }
        for event_type, count in casafari_event_type_rows
    ]

    zone_rows = get_zone_intelligence_v2(session, window_days=14)
    low_confidence_zones = sorted(
        [row for row in zone_rows if row.get("zone_confidence_score", 0) < 40],
        key=lambda row: (
            row.get("zone_confidence_score", 0),
            -(row.get("casafari_raw_in_zone", 0) or 0),
        ),
    )

    low_confidence_zone_rows = [
        {
            "zone_label": row.get("zone_label"),
            "zone_confidence_score": row.get("zone_confidence_score"),
            "casafari_raw_in_zone": row.get("casafari_raw_in_zone"),
            "geo_point_ratio": row.get("geo_point_ratio"),
            "recommended_action": row.get("recommended_action"),
        }
        for row in low_confidence_zones[:8]
    ]

    sync_state = session.scalar(
        select(SourceSyncState).where(SourceSyncState.source_name == CASAFARI_SOURCE_NAME)
    )

    raw_captured_at = [item.captured_at for item in raw_items]
    casafari_event_datetimes = list(
        session.scalars(
            select(MarketEvent.event_datetime).where(
                MarketEvent.source_channel == "casafari"
            )
        ).all()
    )

    return {
        "assets": assets,
        "buildings": buildings,
        "contacts": contacts,
        "listings": listings,
        "events": events,
        "casafari_raw": casafari_raw,
        "casafari_links": total_links,
        "casafari_resolved": casafari_resolved,
        "casafari_ambiguous": casafari_ambiguous,
        "casafari_unresolved": casafari_unresolved,
        "casafari_events": casafari_events,
        "casafari_resolved_ratio": _safe_ratio(casafari_resolved, casafari_raw),
        "casafari_unresolved_ratio": _safe_ratio(casafari_unresolved, casafari_raw),
        "assets_with_district": assets_with_district,
        "assets_with_neighborhood": assets_with_neighborhood,
        "assets_with_geo_point": assets_with_geo_point,
        "raws_with_reliable_price": raws_with_reliable_price,
        "raws_without_reliable_price": max(casafari_raw - raws_with_reliable_price, 0),
        "raws_with_poor_address": raws_with_poor_address,
        "raws_with_precise_or_partial_address": max(casafari_raw - raws_with_poor_address, 0),
        "casafari_raw_7d": _recent_count(raw_captured_at, days=7),
        "casafari_raw_30d": _recent_count(raw_captured_at, days=30),
        "casafari_events_7d": _recent_count(casafari_event_datetimes, days=7),
        "casafari_events_30d": _recent_count(casafari_event_datetimes, days=30),
        "event_type_breakdown": event_type_breakdown,
        "low_confidence_zones_count": len(low_confidence_zones),
        "low_confidence_zones": low_confidence_zone_rows,
        "last_sync_status": sync_state.last_status if sync_state else None,
        "last_sync_started_at": sync_state.last_started_at if sync_state else None,
        "last_sync_finished_at": sync_state.last_finished_at if sync_state else None,
        "last_sync_from": sync_state.last_success_from if sync_state else None,
        "last_sync_to": sync_state.last_success_to if sync_state else None,
        "last_sync_item_count": sync_state.last_item_count if sync_state else None,
        "last_sync_message": sync_state.last_message if sync_state else None,
    }
