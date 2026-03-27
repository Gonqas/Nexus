from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import core.services.copilot_service as copilot_service
from db.base import Base
import db.models  # noqa: F401


def make_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    return Session()


def test_copilot_zone_transformation_intent(monkeypatch) -> None:
    session = make_session()
    try:
        monkeypatch.setattr(
            copilot_service,
            "get_zone_intelligence_v2",
            lambda session, window_days=14: [
                {
                    "zone_label": "Cortes",
                    "zone_transformation_signal_score": 71.0,
                    "zone_capture_score": 58.0,
                    "zone_heat_score": 61.0,
                    "zone_relative_heat_score": 66.0,
                    "predicted_absorption_30d_score": 57.0,
                    "ai_brief": "Transformacion urbana clara.",
                    "ai_next_step": "Seguir y vigilar.",
                    "executive_summary": "Cortes va fuerte.",
                    "recommended_action": "Seguir y vigilar",
                }
            ],
        )

        payload = copilot_service.run_copilot_query(session, "que barrios tienen transformacion")

        assert payload["intent"] == "zone_transformation"
        assert payload["suggestions"][0]["item"] == "Cortes"
        assert payload["suggestions"][0]["target_view"] == "radar"
    finally:
        session.close()


def test_copilot_casafari_review_intent(monkeypatch) -> None:
    session = make_session()
    try:
        monkeypatch.setattr(
            copilot_service,
            "list_casafari_links",
            lambda session, status_filter="all", focus_filter="all", query_text=None, limit=20: [
                {
                    "address_raw": "calle Alcala 10",
                    "ai_brief": "Faltan senales de identidad.",
                    "ai_next_step": "No enlazar aun.",
                }
            ],
        )

        payload = copilot_service.run_copilot_query(session, "casafari weak identity")

        assert payload["intent"] == "casafari_review"
        assert payload["suggestions"][0]["tipo"] == "Casafari"
        assert payload["suggestions"][0]["target_view"] == "casafari"
    finally:
        session.close()


def test_copilot_falls_back_to_search(monkeypatch) -> None:
    session = make_session()
    try:
        monkeypatch.setattr(
            copilot_service,
            "search_payload",
            lambda session, query, section_filter="all", limit_per_section=5: {
                "summary": {"assets": 1, "listings": 0, "raws": 0, "events": 0, "total": 1},
                "assets": [{"asset_id": 1}],
                "listings": [],
                "raws": [],
                "events": [],
                "index_status": {"backend": "fts5", "doc_count": 10},
            },
        )

        payload = copilot_service.run_copilot_query(session, "calle mayor 10")

        assert payload["intent"] == "search_fallback"
        assert payload["search_payload"]["summary"]["total"] == 1
    finally:
        session.close()


def test_copilot_detects_operational_action() -> None:
    session = make_session()
    try:
        payload = copilot_service.run_copilot_query(session, "reconciliar pendientes casafari")

        assert payload["intent"] == "action_reconcile"
        assert payload["suggestions"][0]["action_id"] == "casafari_reconcile"
        assert payload["suggestions"][0]["target_view"] == "casafari"
    finally:
        session.close()
