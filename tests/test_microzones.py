from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.features.microzones import infer_microzone_for_asset, microzone_cell_code
import core.services.microzone_intelligence_service as microzone_intelligence_service
from db.base import Base
from db.models.asset import Asset
from db.models.listing import Listing
from db.models.market_event import MarketEvent
import db.models  # noqa: F401


def make_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    return Session()


def test_microzone_code_is_stable_for_nearby_points() -> None:
    code_a = microzone_cell_code(40.41680, -3.70380)
    code_b = microzone_cell_code(40.41710, -3.70400)
    code_c = microzone_cell_code(40.42050, -3.71050)

    assert code_a is not None
    assert code_a == code_b
    assert code_c != code_a


def test_microzone_intelligence_builds_local_hotspots(monkeypatch) -> None:
    session = make_session()
    try:
        now_dt = datetime.now(timezone.utc).replace(tzinfo=None)

        asset_1 = Asset(
            asset_type_detail="piso",
            address_raw="Calle Mayor 10",
            neighborhood="Sol",
            district="Centro",
            lat=40.41680,
            lon=-3.70380,
        )
        asset_2 = Asset(
            asset_type_detail="piso",
            address_raw="Calle Mayor 14",
            neighborhood="Sol",
            district="Centro",
            lat=40.41700,
            lon=-3.70395,
        )
        asset_3 = Asset(
            asset_type_detail="piso",
            address_raw="Calle Luchana 12",
            neighborhood="Trafalgar",
            district="Chamberi",
            lat=40.43050,
            lon=-3.70120,
        )

        session.add_all(
            [
                asset_1,
                asset_2,
                asset_3,
                Listing(asset=asset_1, price_per_m2=6000, status="active"),
                Listing(asset=asset_2, price_per_m2=6100, status="active"),
                Listing(asset=asset_3, price_per_m2=6300, status="active"),
                MarketEvent(
                    asset=asset_1,
                    event_type="listing_detected",
                    event_datetime=now_dt - timedelta(days=1),
                    source_channel="casafari",
                ),
                MarketEvent(
                    asset=asset_2,
                    event_type="price_drop",
                    event_datetime=now_dt - timedelta(days=2),
                    source_channel="casafari",
                ),
                MarketEvent(
                    asset=asset_3,
                    event_type="listing_detected",
                    event_datetime=now_dt - timedelta(days=3),
                    source_channel="casafari",
                ),
            ]
        )
        session.commit()

        monkeypatch.setattr(
            microzone_intelligence_service,
            "get_zone_intelligence_v2",
            lambda session, window_days=14: [
                {
                    "zone_label": "Sol",
                    "assets_count": 2,
                    "events_14d": 2,
                    "zone_capture_score": 72.0,
                    "zone_relative_heat_score": 68.0,
                    "zone_confidence_score": 74.0,
                    "zone_transformation_signal_score": 41.0,
                    "recommended_action": "Captacion selectiva",
                },
                {
                    "zone_label": "Trafalgar",
                    "assets_count": 1,
                    "events_14d": 1,
                    "zone_capture_score": 48.0,
                    "zone_relative_heat_score": 44.0,
                    "zone_confidence_score": 52.0,
                    "zone_transformation_signal_score": 30.0,
                    "recommended_action": "Seguir",
                },
            ],
        )

        rows = microzone_intelligence_service.get_microzone_intelligence(
            session,
            window_days=14,
        )

        assert len(rows) >= 2
        top = rows[0]
        assert top["microzone_label"]
        assert top["parent_zone_label"] == "Sol"
        assert top["microzone_capture_score"] >= 0
        assert top["microzone_concentration_score"] >= 0
        assert top["recommended_action"]
        assert infer_microzone_for_asset(asset_1) == infer_microzone_for_asset(asset_2)
    finally:
        session.close()
