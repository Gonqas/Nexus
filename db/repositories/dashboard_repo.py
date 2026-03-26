from sqlalchemy import func, select
from sqlalchemy.orm import Session

from db.models.asset import Asset
from db.models.building import Building
from db.models.casafari_event_link import CasafariEventLink
from db.models.contact import Contact
from db.models.listing import Listing
from db.models.market_event import MarketEvent
from db.models.raw_history_item import RawHistoryItem


def get_dashboard_stats(session: Session) -> dict[str, int]:
    assets = int(session.scalar(select(func.count(Asset.id))) or 0)
    buildings = int(session.scalar(select(func.count(Building.id))) or 0)
    contacts = int(session.scalar(select(func.count(Contact.id))) or 0)
    listings = int(session.scalar(select(func.count(Listing.id))) or 0)
    events = int(session.scalar(select(func.count(MarketEvent.id))) or 0)

    casafari_raw = int(
        session.scalar(
            select(func.count(RawHistoryItem.id)).where(
                RawHistoryItem.source_name == "casafari_history"
            )
        )
        or 0
    )

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

    return {
        "assets": assets,
        "buildings": buildings,
        "contacts": contacts,
        "listings": listings,
        "events": events,
        "casafari_raw": casafari_raw,
        "casafari_resolved": casafari_resolved,
        "casafari_ambiguous": casafari_ambiguous,
        "casafari_unresolved": casafari_unresolved,
        "casafari_events": casafari_events,
    }