from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.config.settings import CASAFARI_SOURCE_NAME
from db.base import Base
from db.models.asset import Asset
from db.models.casafari_event_link import CasafariEventLink
from db.models.market_event import MarketEvent
from db.models.raw_history_item import RawHistoryItem
from db.models.source_sync_state import SourceSyncState
import db.models  # noqa: F401
import db.repositories.dashboard_repo as dashboard_repo


def make_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    return Session()


def test_dashboard_stats_expose_quality_ratios_breakdowns_and_sync(monkeypatch) -> None:
    session = make_session()
    try:
        now_dt = datetime.now(timezone.utc).replace(tzinfo=None)

        asset = Asset(
            address_raw="Calle Mayor 10",
            address_norm="calle mayor 10",
            district="Centro",
            neighborhood="Sol",
            lat=40.4168,
            lon=-3.7038,
        )

        raw_precise = RawHistoryItem(
            source_name=CASAFARI_SOURCE_NAME,
            source_uid="raw-1",
            event_type_guess="price_drop",
            event_datetime=now_dt - timedelta(days=1),
            captured_at=now_dt - timedelta(days=1),
            address_raw="Calle Mayor 10",
            current_price_eur=350000,
            raw_text="Bajada de precio € 350.000",
        )
        raw_zone_like = RawHistoryItem(
            source_name=CASAFARI_SOURCE_NAME,
            source_uid="raw-2",
            event_type_guess="listing_detected",
            event_datetime=now_dt - timedelta(days=3),
            captured_at=now_dt - timedelta(days=3),
            address_raw="Salamanca",
            current_price_eur=None,
            raw_text="Nuevo anuncio en Salamanca",
        )

        session.add_all(
            [
                asset,
                raw_precise,
                raw_zone_like,
                CasafariEventLink(
                    raw_history_item=raw_precise,
                    match_status="resolved",
                    match_score=96.0,
                ),
                CasafariEventLink(
                    raw_history_item=raw_zone_like,
                    match_status="unresolved",
                    match_score=0.0,
                ),
                MarketEvent(
                    asset=asset,
                    event_type="price_drop",
                    event_datetime=now_dt - timedelta(days=1),
                    source_channel="casafari",
                    price_new=350000,
                ),
                MarketEvent(
                    asset=asset,
                    event_type="listing_detected",
                    event_datetime=now_dt - timedelta(days=10),
                    source_channel="casafari",
                    price_new=360000,
                ),
                SourceSyncState(
                    source_name=CASAFARI_SOURCE_NAME,
                    last_status="success",
                    last_started_at=now_dt - timedelta(hours=2),
                    last_finished_at=now_dt - timedelta(hours=1),
                    last_success_from=now_dt - timedelta(days=2),
                    last_success_to=now_dt,
                    last_item_count=12,
                    last_message="sync ok",
                ),
            ]
        )
        session.commit()

        monkeypatch.setattr(
            dashboard_repo,
            "get_zone_intelligence_v2",
            lambda session, window_days=14: [
                {
                    "zone_label": "Zona floja",
                    "zone_confidence_score": 22.5,
                    "casafari_raw_in_zone": 7,
                    "geo_point_ratio": 0.15,
                    "recommended_action": "Poca señal / baja confianza",
                },
                {
                    "zone_label": "Zona sana",
                    "zone_confidence_score": 64.0,
                    "casafari_raw_in_zone": 3,
                    "geo_point_ratio": 0.80,
                    "recommended_action": "Seguir",
                },
            ],
        )

        stats = dashboard_repo.get_dashboard_stats(session)

        assert stats["casafari_raw"] == 2
        assert stats["casafari_resolved"] == 1
        assert stats["casafari_unresolved"] == 1
        assert stats["casafari_resolved_ratio"] == 0.5
        assert stats["casafari_unresolved_ratio"] == 0.5
        assert stats["raws_without_reliable_price"] == 1
        assert stats["raws_with_poor_address"] == 1
        assert stats["casafari_raw_7d"] == 2
        assert stats["casafari_events_7d"] == 1
        assert stats["casafari_events_30d"] == 2
        assert stats["low_confidence_zones_count"] == 1
        assert stats["low_confidence_zones"][0]["zone_label"] == "Zona floja"
        assert stats["event_type_breakdown"][0]["count"] == 1
        assert stats["last_sync_status"] == "success"
        assert stats["last_sync_item_count"] == 12
    finally:
        session.close()
