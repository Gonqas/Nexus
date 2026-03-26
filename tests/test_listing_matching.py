from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.identity.listing_resolver import find_existing_listing
from core.normalization.addresses import normalize_address_key
from core.normalization.phones import normalize_phone
from core.services.casafari_reconciliation_service import resolve_raw_item
from core.services.import_service import get_or_create_listing
from db.base import Base
from db.models.asset import Asset
from db.models.contact import Contact
from db.models.listing import Listing
from db.models.raw_history_item import RawHistoryItem
import db.models  # noqa: F401


def make_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    return Session()


def test_get_or_create_listing_canonicalizes_csv_portal_label() -> None:
    session = make_session()
    try:
        asset = Asset(address_raw="Calle Mayor 10", address_norm=normalize_address_key("Calle Mayor 10"))
        session.add(asset)
        session.flush()

        listing, created = get_or_create_listing(
            session,
            asset=asset,
            contact=None,
            row={
                "Página del anuncio": "https://www.fotocasa.es/es/comprar/vivienda/madrid-capital/calle-mayor-10/123456789/d",
                "Página de la propiedad": None,
                "Fuente": "Fotocasa for Sale",
                "Estado del anuncio": "Activo",
                "Precio": "350000",
                "Área construida": "85",
                "Fecha de creación del lead": "2026-03-20",
            },
        )

        assert created is True
        assert listing.source_portal == "Fotocasa"
    finally:
        session.close()


def test_find_existing_listing_matches_same_asset_across_portal_variants() -> None:
    session = make_session()
    try:
        asset = Asset(address_raw="Calle Mayor 10", address_norm=normalize_address_key("Calle Mayor 10"))
        listing = Listing(asset=asset, source_portal="Fotocasa")
        session.add_all([asset, listing])
        session.flush()

        found = find_existing_listing(
            session=session,
            listing_url=None,
            property_url=None,
            external_id=None,
            source_portal="Fotocasa for Sale",
            asset_id=asset.id,
            contact_id=None,
        )

        assert found is not None
        assert found.id == listing.id
    finally:
        session.close()


def test_resolve_raw_item_uses_exact_address_and_phone_to_link_casafari_alert() -> None:
    session = make_session()
    try:
        asset = Asset(
            address_raw="Calle Mayor 10",
            address_norm=normalize_address_key("Calle Mayor 10"),
        )
        contact = Contact(
            phone_raw="699111222",
            phone_norm=normalize_phone("699111222"),
            name_raw="Ana",
            name_norm="ana",
        )
        listing = Listing(
            asset=asset,
            contact=contact,
            source_portal="Fotocasa",
            price_eur=350000,
            origin_channel="csv",
        )
        raw_item = RawHistoryItem(
            source_name="casafari_history",
            source_uid="test-source-uid-1",
            event_type_guess="price_drop",
            event_datetime=datetime(2026, 3, 25, 12, 0, 0),
            address_raw="Calle Mayor 10",
            portal="Fotocasa : Anuncio Particular",
            contact_phone="699111222",
            current_price_eur=349000,
            raw_text="Bajada de precio en Calle Mayor 10",
        )

        session.add_all([asset, contact, listing, raw_item])
        session.flush()

        status, event_created, _strategy, score = resolve_raw_item(session, raw_item)

        assert status == "resolved"
        assert event_created is True
        assert score is not None and score >= 75
    finally:
        session.close()
