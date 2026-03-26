from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import core.services.opportunity_queue_service_v2 as opportunity_queue_service_v2
from core.services.opportunity_queue_detail_service_v2 import get_opportunity_detail_v2
from core.services.opportunity_queue_service_v2 import (
    apply_group_selection,
    build_opportunity_groups,
    filter_opportunity_rows,
    get_opportunity_queue_v2,
)
from db.base import Base
from db.models.asset import Asset
from db.models.contact import Contact
from db.models.listing import Listing
from db.models.market_event import MarketEvent
import db.models  # noqa: F401


def make_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    return Session()


def _seed_queue_data(session):
    now_dt = datetime.now(timezone.utc).replace(tzinfo=None)

    contact_a = Contact(phone_raw="+34699111222", phone_norm="699111222", name_raw="Ana", name_norm="ana")
    contact_b = Contact(phone_raw="+34699111222", phone_norm="699111222", name_raw="Ana 2", name_norm="ana 2")
    contact_c = Contact(phone_raw="+34655333444", phone_norm="655333444", name_raw="Luis", name_norm="luis")

    asset_1 = Asset(
        asset_type_detail="piso",
        address_raw="Calle Mayor 10",
        neighborhood="Sol",
        district="Centro",
        area_m2=80,
        lat=40.4168,
        lon=-3.7038,
    )
    asset_2 = Asset(
        asset_type_detail="piso",
        address_raw="Calle Arenal 8",
        neighborhood="Sol",
        district="Centro",
        area_m2=82,
        lat=40.4171,
        lon=-3.7041,
    )
    asset_3 = Asset(
        asset_type_detail="piso",
        address_raw="Calle Luchana 12",
        neighborhood="Trafalgar",
        district="Chamberi",
        area_m2=95,
        lat=None,
        lon=None,
    )

    listing_1 = Listing(asset=asset_1, contact=contact_a, source_portal="Idealista", price_eur=450000, price_per_m2=5625)
    listing_2 = Listing(asset=asset_2, contact=contact_b, source_portal="Fotocasa", price_eur=470000, price_per_m2=5731.7)
    listing_3 = Listing(asset=asset_3, contact=contact_c, source_portal="Pisos", price_eur=620000, price_per_m2=6526.3)

    event_1 = MarketEvent(
        asset=asset_1,
        listing=listing_1,
        event_type="listing_detected",
        event_datetime=now_dt - timedelta(days=1),
        source_channel="casafari",
        price_new=450000,
    )
    event_2 = MarketEvent(
        asset=asset_2,
        listing=listing_2,
        event_type="price_drop",
        event_datetime=now_dt - timedelta(days=2),
        source_channel="casafari",
        price_old=520000,
        price_new=470000,
    )
    event_3 = MarketEvent(
        asset=asset_3,
        listing=listing_3,
        event_type="sold",
        event_datetime=now_dt - timedelta(days=3),
        source_channel="casafari",
        price_new=620000,
    )

    session.add_all(
        [
            contact_a,
            contact_b,
            contact_c,
            asset_1,
            asset_2,
            asset_3,
            listing_1,
            listing_2,
            listing_3,
            event_1,
            event_2,
            event_3,
        ]
    )
    session.commit()

    return {
        "event_1": event_1,
        "event_2": event_2,
        "event_3": event_3,
    }


def test_opportunity_queue_filters_groups_and_breakdown(monkeypatch) -> None:
    session = make_session()
    try:
        seeded = _seed_queue_data(session)

        monkeypatch.setattr(
            opportunity_queue_service_v2,
            "infer_zone_label_for_asset",
            lambda asset: asset.neighborhood or "Sin zona",
        )
        monkeypatch.setattr(
            opportunity_queue_service_v2,
            "_zone_map",
            lambda session, window_days=14: {
                "Sol": {
                    "zone_capture_score": 70.0,
                    "zone_pressure_score": 55.0,
                    "zone_confidence_score": 75.0,
                    "recommended_action": "Captación selectiva",
                },
                "Trafalgar": {
                    "zone_capture_score": 48.0,
                    "zone_pressure_score": 42.0,
                    "zone_confidence_score": 40.0,
                    "recommended_action": "Seguir",
                },
            },
        )
        monkeypatch.setattr(
            opportunity_queue_service_v2,
            "_microzone_map",
            lambda session, window_days=14: {
                "Sol / MZ mx-my": {
                    "microzone_label": "Sol / MZ mx-my",
                    "microzone_capture_score": 72.0,
                    "microzone_concentration_score": 68.0,
                    "microzone_confidence_score": 61.0,
                    "recommended_action": "Ir al punto caliente",
                }
            },
        )
        monkeypatch.setattr(
            opportunity_queue_service_v2,
            "infer_microzone_for_asset",
            lambda asset: "Sol / MZ mx-my" if asset and asset.neighborhood == "Sol" else None,
        )

        rows = get_opportunity_queue_v2(session, window_days=14, limit=20)

        assert len(rows) == 3
        assert rows[0]["priority_label"] in {"alta", "media", "seguimiento"}
        assert "evento" in rows[0]["score_breakdown"]
        assert rows[0]["microzone_label"] == "Sol / MZ mx-my"
        assert rows[0]["score_microzone_signal"] > 0

        filtered = filter_opportunity_rows(
            rows,
            event_type_filter="price_drop",
            geo_filter="with_geo",
            min_score=40.0,
            zone_query="sol",
        )
        assert len(filtered) == 1
        assert filtered[0]["event_id"] == seeded["event_2"].id

        groups = build_opportunity_groups(rows, group_by="contact", limit=10)
        assert groups[0]["events_count"] == 2
        assert "699111222" in groups[0]["group_key"]

        selected = apply_group_selection(
            rows,
            group_by="contact",
            group_key=groups[0]["group_key"],
        )
        assert len(selected) == 2
    finally:
        session.close()


def test_opportunity_detail_includes_comparables(monkeypatch) -> None:
    session = make_session()
    try:
        seeded = _seed_queue_data(session)

        monkeypatch.setattr(
            opportunity_queue_service_v2,
            "infer_zone_label_for_asset",
            lambda asset: asset.neighborhood or "Sin zona",
        )
        monkeypatch.setattr(
            opportunity_queue_service_v2,
            "_zone_map",
            lambda session, window_days=14: {
                "Sol": {
                    "zone_capture_score": 70.0,
                    "zone_pressure_score": 55.0,
                    "zone_confidence_score": 75.0,
                    "recommended_action": "Captación selectiva",
                },
                "Trafalgar": {
                    "zone_capture_score": 48.0,
                    "zone_pressure_score": 42.0,
                    "zone_confidence_score": 40.0,
                    "recommended_action": "Seguir",
                },
            },
        )
        monkeypatch.setattr(
            opportunity_queue_service_v2,
            "_microzone_map",
            lambda session, window_days=14: {
                "Sol / MZ mx-my": {
                    "microzone_label": "Sol / MZ mx-my",
                    "microzone_capture_score": 72.0,
                    "microzone_concentration_score": 68.0,
                    "microzone_confidence_score": 61.0,
                    "recommended_action": "Ir al punto caliente",
                }
            },
        )
        monkeypatch.setattr(
            opportunity_queue_service_v2,
            "infer_microzone_for_asset",
            lambda asset: "Sol / MZ mx-my" if asset and asset.neighborhood == "Sol" else None,
        )

        detail = get_opportunity_detail_v2(session, seeded["event_1"].id, window_days=14)

        assert detail["found"] is True
        assert detail["queue_row"]["event_id"] == seeded["event_1"].id
        assert detail["queue_row"]["microzone_label"] == "Sol / MZ mx-my"
        assert detail["comparables"]["summary"]["comparables_count"] >= 1
    finally:
        session.close()
