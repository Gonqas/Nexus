from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import core.services.spatial_map_service as spatial_map_service
from db.base import Base
import db.models  # noqa: F401


def make_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    return Session()


def test_spatial_map_payload_exposes_viewport_and_layers(monkeypatch) -> None:
    session = make_session()
    try:
        monkeypatch.setattr(
            spatial_map_service,
            "get_opportunity_queue_v2",
            lambda session, window_days=14, limit=400: [
                {
                    "event_id": 10,
                    "asset_lat": 40.42,
                    "asset_lon": -3.70,
                    "score": 71.2,
                    "priority_label": "alta",
                    "event_type": "listing_detected",
                    "zone_label": "Prosperidad",
                    "microzone_label": "Prosperidad / MZ a-b",
                    "portal": "Idealista",
                    "asset_address": "Calle Lopez de Hoyos 12",
                    "reason": "entrada reciente | geo por barrio",
                    "zone_recommended_action": "Seguir y vigilar",
                    "predicted_opportunity_30d_band": "moderada",
                    "predicted_opportunity_30d_score": 55.1,
                    "has_geo_point": True,
                },
                {
                    "event_id": 11,
                    "asset_lat": 40.43,
                    "asset_lon": -3.69,
                    "score": 53.0,
                    "priority_label": "media",
                    "event_type": "price_drop",
                    "zone_label": "Guindalera",
                    "microzone_label": None,
                    "portal": "Fotocasa",
                    "asset_address": "Calle Cartagena 8",
                    "reason": "bajada de precio",
                    "zone_recommended_action": "Captacion selectiva",
                    "predicted_opportunity_30d_band": "moderada",
                    "predicted_opportunity_30d_score": 48.0,
                    "has_geo_point": True,
                },
            ],
        )
        monkeypatch.setattr(
            spatial_map_service,
            "filter_opportunity_rows",
            lambda rows, **kwargs: rows,
        )
        monkeypatch.setattr(
            spatial_map_service,
            "get_microzone_intelligence",
            lambda session, window_days=14, limit=None: [
                {
                    "microzone_label": "Prosperidad / MZ a-b",
                    "parent_zone_label": "Prosperidad",
                    "centroid_lat": 40.4205,
                    "centroid_lon": -3.7005,
                    "microzone_capture_score": 74.0,
                    "microzone_concentration_score": 69.0,
                    "microzone_confidence_score": 62.0,
                    "events_14d": 4,
                    "recommended_action": "Ir al punto caliente",
                    "radar_explanation": "Hotspot micro.",
                }
            ],
        )

        payload = spatial_map_service.get_spatial_map_payload(
            session,
            window_days=30,
            event_type_filter="all",
            min_score=50.0,
            zone_query="pro",
            layer_mode="both",
        )

        assert payload["window_days"] == 30
        assert payload["summary"]["geo_opportunities_total"] == 2
        assert payload["summary"]["high_priority_geo_opportunities"] == 1
        assert payload["summary"]["microzones_total"] == 1
        assert payload["viewport"]["center"]["lat"] > 40.42
        assert payload["viewport"]["bounds"]["west"] < -3.69
        assert payload["points"][0]["event_id"] == 10
        assert payload["microzones"][0]["microzone_label"] == "Prosperidad / MZ a-b"
    finally:
        session.close()


def test_spatial_map_payload_can_focus_single_layer(monkeypatch) -> None:
    session = make_session()
    try:
        monkeypatch.setattr(
            spatial_map_service,
            "get_opportunity_queue_v2",
            lambda session, window_days=14, limit=400: [],
        )
        monkeypatch.setattr(
            spatial_map_service,
            "filter_opportunity_rows",
            lambda rows, **kwargs: rows,
        )
        monkeypatch.setattr(
            spatial_map_service,
            "get_microzone_intelligence",
            lambda session, window_days=14, limit=None: [
                {
                    "microzone_label": "Arapiles / MZ c-d",
                    "parent_zone_label": "Arapiles",
                    "centroid_lat": 40.433,
                    "centroid_lon": -3.705,
                    "microzone_capture_score": 67.0,
                    "microzone_concentration_score": 58.0,
                    "microzone_confidence_score": 61.0,
                    "events_14d": 3,
                    "recommended_action": "Seguir de cerca",
                    "radar_explanation": "Microzona activa.",
                }
            ],
        )

        payload = spatial_map_service.get_spatial_map_payload(
            session,
            layer_mode="microzones",
        )

        assert payload["points"] == []
        assert len(payload["microzones"]) == 1
        assert payload["viewport"]["bounds"]["north"] == 40.433
    finally:
        session.close()
