from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import core.services.radar_service_v2 as radar_service_v2
from db.base import Base
import db.models  # noqa: F401


def make_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    return Session()


def test_radar_payload_exposes_window_summary_and_rank_tables(monkeypatch) -> None:
    session = make_session()
    try:
        monkeypatch.setattr(
            radar_service_v2,
            "get_zone_intelligence_v2",
            lambda session, window_days=14: [
                {
                    "zone_label": "Zona A",
                    "zone_capture_score": 88.0,
                    "zone_heat_score": 71.0,
                    "zone_pressure_score": 66.0,
                    "zone_liquidity_score": 60.0,
                    "zone_confidence_score": 78.0,
                    "recommended_action": "Captación agresiva",
                    "executive_summary": "Muy fuerte",
                    "events_14d": 8,
                    "price_drop_count": 3,
                    "absorption_count": 5,
                    "casafari_raw_in_zone": 7,
                },
                {
                    "zone_label": "Zona B",
                    "zone_capture_score": 62.0,
                    "zone_heat_score": 45.0,
                    "zone_pressure_score": 52.0,
                    "zone_liquidity_score": 48.0,
                    "zone_confidence_score": 55.0,
                    "recommended_action": "Captación selectiva",
                    "executive_summary": "Buena",
                    "events_14d": 5,
                    "price_drop_count": 2,
                    "absorption_count": 2,
                    "casafari_raw_in_zone": 4,
                },
                {
                    "zone_label": "Zona C",
                    "zone_capture_score": 40.0,
                    "zone_heat_score": 30.0,
                    "zone_pressure_score": 25.0,
                    "zone_liquidity_score": 28.0,
                    "zone_confidence_score": 22.0,
                    "recommended_action": "Poca señal / baja confianza",
                    "executive_summary": "Floja",
                    "events_14d": 2,
                    "price_drop_count": 0,
                    "absorption_count": 1,
                    "casafari_raw_in_zone": 10,
                },
                {
                    "zone_label": "Zona D",
                    "zone_capture_score": 999.0,
                    "zone_heat_score": 999.0,
                    "zone_pressure_score": 999.0,
                    "zone_liquidity_score": 999.0,
                    "zone_confidence_score": 65.0,
                    "recommended_action": "Captación agresiva",
                    "executive_summary": "Outlier",
                    "events_14d": 1,
                    "price_drop_count": 1,
                    "absorption_count": 1,
                    "casafari_raw_in_zone": 1,
                },
                {
                    "zone_label": "Zona E",
                    "zone_capture_score": 58.0,
                    "zone_heat_score": 62.0,
                    "zone_pressure_score": 50.0,
                    "zone_liquidity_score": 46.0,
                    "zone_confidence_score": 61.0,
                    "recommended_action": "Seguir y vigilar",
                    "executive_summary": "Activa",
                    "events_14d": 6,
                    "price_drop_count": 1,
                    "absorption_count": 2,
                    "casafari_raw_in_zone": 2,
                },
                {
                    "zone_label": "Zona F",
                    "zone_capture_score": 55.0,
                    "zone_heat_score": 67.0,
                    "zone_pressure_score": 48.0,
                    "zone_liquidity_score": 44.0,
                    "zone_confidence_score": 39.0,
                    "recommended_action": "Seguir y vigilar",
                    "executive_summary": "Caliente pero floja",
                    "events_14d": 7,
                    "price_drop_count": 1,
                    "absorption_count": 2,
                    "casafari_raw_in_zone": 5,
                },
            ],
        )
        monkeypatch.setattr(
            radar_service_v2,
            "get_microzone_intelligence",
            lambda session, window_days=14, limit=16: [
                {
                    "zone_label": "Zona A / MZ p1-p2",
                    "microzone_label": "Zona A / MZ p1-p2",
                    "microzone_capture_score": 74.0,
                    "microzone_concentration_score": 71.0,
                    "microzone_confidence_score": 66.0,
                    "recommended_action": "Ir al punto caliente",
                    "executive_summary": "Hotspot micro.",
                    "events_14d": 4,
                },
                {
                    "zone_label": "Zona B / MZ p1-p3",
                    "microzone_label": "Zona B / MZ p1-p3",
                    "microzone_capture_score": 58.0,
                    "microzone_concentration_score": 49.0,
                    "microzone_confidence_score": 52.0,
                    "recommended_action": "Seguir de cerca",
                    "executive_summary": "Seguimiento micro.",
                    "events_14d": 2,
                },
            ],
        )

        payload = radar_service_v2.get_radar_payload_v2(session, window_days=30)

        assert payload["window_days"] == 30
        assert payload["summary"]["zones_total"] == 6
        assert payload["summary"]["high_confidence_zones"] == 3
        assert payload["summary"]["low_confidence_zones"] == 2
        assert payload["summary"]["capture_ready_zones"] == 3
        assert payload["summary"]["hot_zones"] == 3
        assert payload["summary"]["microzones_total"] == 2
        assert payload["summary"]["microzone_hotspots"] == 1
        assert payload["top_capture"][0]["radar_explanation"]
        assert payload["top_microzones"][0]["microzone_capture_score"] == 74.0
        assert payload["low_confidence"][0]["zone_confidence_score"] <= payload["low_confidence"][1]["zone_confidence_score"]
    finally:
        session.close()
