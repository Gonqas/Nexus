from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.services.search_service import ensure_search_index, search_payload
from db.base import Base
from db.models.asset import Asset
from db.models.casafari_event_link import CasafariEventLink
from db.models.contact import Contact
from db.models.listing import Listing
from db.models.market_event import MarketEvent
from db.models.raw_history_item import RawHistoryItem
import db.models  # noqa: F401


def make_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    return Session()


def test_search_payload_finds_assets_listings_raws_and_events_by_phone_and_reason() -> None:
    session = make_session()
    try:
        now_dt = datetime.now(timezone.utc).replace(tzinfo=None)

        asset = Asset(
            address_raw="Calle Mayor 10",
            address_norm="calle mayor 10",
            neighborhood="Sol",
            district="Centro",
        )
        contact = Contact(
            phone_raw="699111222",
            phone_norm="699111222",
            name_raw="Ana",
            name_norm="ana",
        )
        listing = Listing(
            asset=asset,
            contact=contact,
            source_portal="Fotocasa",
            price_eur=350000,
            listing_url="https://portal.test/anuncio-1",
        )
        raw_item = RawHistoryItem(
            source_name="casafari_history",
            source_uid="search-raw-1",
            event_type_guess="listing_detected",
            event_datetime=now_dt,
            address_raw="Calle Mayor 10",
            contact_name="Ana",
            contact_phone="699111222",
            portal="Fotocasa",
            raw_text="Nuevo anuncio con telefono 699111222",
        )
        event = MarketEvent(
            asset=asset,
            listing=listing,
            event_type="listing_detected",
            event_datetime=now_dt,
            source_channel="casafari",
            price_new=350000,
            raw_text="listing_detected Calle Mayor 10",
        )
        link = CasafariEventLink(
            raw_history_item=raw_item,
            listing=listing,
            asset=asset,
            contact=contact,
            match_status="unresolved",
            match_note="reason=weak_identity | falta una URL fuerte",
            match_score=0.0,
        )

        session.add_all([asset, contact, listing, raw_item, event, link])
        session.commit()

        phone_results = search_payload(session, "699111222")
        assert phone_results["index_status"]["backend"] == "fts5"
        assert phone_results["summary"]["listings"] == 1
        assert phone_results["summary"]["raws"] == 1

        reason_results = search_payload(session, "weak_identity", section_filter="raws")
        assert reason_results["summary"]["raws"] == 1
        assert reason_results["raws"][0]["reason_taxonomy"] == "weak_identity"
        assert reason_results["raws"][0]["snippet"]

        address_results = search_payload(session, "calle mayor 10")
        assert address_results["summary"]["assets"] == 1
        assert address_results["summary"]["events"] == 1
    finally:
        session.close()


def test_search_index_rebuilds_automatically_when_source_data_changes() -> None:
    session = make_session()
    try:
        ensure_search_index(session, force_rebuild=True)

        asset = Asset(
            address_raw="Calle Serrano 45",
            address_norm="calle serrano 45",
            neighborhood="Recoletos",
            district="Salamanca",
        )
        session.add(asset)
        session.commit()

        results = search_payload(session, "serrano 45", section_filter="assets")

        assert results["summary"]["assets"] == 1
        assert results["assets"][0]["address"] == "Calle Serrano 45"
    finally:
        session.close()
