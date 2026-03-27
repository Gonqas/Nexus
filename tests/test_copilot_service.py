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
        assert len(payload["followups"]) >= 1
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


def test_copilot_can_explain_selected_zone_from_context(monkeypatch) -> None:
    session = make_session()
    try:
        monkeypatch.setattr(
            copilot_service,
            "get_zone_intelligence_v2",
            lambda session, window_days=14: [
                {
                    "zone_label": "Prosperidad",
                    "ai_summary": "Prosperidad combina actividad reciente, confianza alta y lectura razonable para captar.",
                    "ai_brief": "Zona fuerte para captar.",
                    "ai_next_step": "Abrir en mapa y validar microzonas.",
                    "executive_summary": "Prosperidad va fuerte.",
                    "recommended_action": "Seguir y vigilar",
                }
            ],
        )

        payload = copilot_service.run_copilot_query(
            session,
            "explicamela mejor",
            context={
                "selected_row": {
                    "target_view": "radar",
                    "zone_label": "Prosperidad",
                    "tipo": "Zona",
                    "item": "Prosperidad",
                }
            },
        )

        assert payload["intent"] == "context_explain"
        assert "Prosperidad" in payload["title"]
        assert payload["suggestions"][0]["zone_label"] == "Prosperidad"
    finally:
        session.close()


def test_copilot_can_compare_zone_using_context(monkeypatch) -> None:
    session = make_session()
    try:
        monkeypatch.setattr(
            copilot_service,
            "get_zone_intelligence_v2",
            lambda session, window_days=14: [
                {
                    "zone_label": "Prosperidad",
                    "zone_capture_score": 61.0,
                    "zone_confidence_score": 78.0,
                    "zone_transformation_signal_score": 21.0,
                    "ai_brief": "Buena lectura comercial.",
                    "ai_next_step": "Seguir y vigilar.",
                    "executive_summary": "Prosperidad va fuerte.",
                    "recommended_action": "Seguir y vigilar",
                },
                {
                    "zone_label": "Guindalera",
                    "zone_capture_score": 54.0,
                    "zone_confidence_score": 57.0,
                    "zone_transformation_signal_score": 18.0,
                    "ai_brief": "Lectura razonable, algo mas fragil.",
                    "ai_next_step": "Validar confianza.",
                    "executive_summary": "Guindalera aguanta.",
                    "recommended_action": "Validar confianza",
                },
            ],
        )

        payload = copilot_service.run_copilot_query(
            session,
            "comparala con guindalera",
            context={
                "selected_row": {
                    "target_view": "radar",
                    "zone_label": "Prosperidad",
                    "tipo": "Zona",
                    "item": "Prosperidad",
                },
                "recent_zone_labels": ["Prosperidad", "Guindalera"],
            },
        )

        assert payload["intent"] == "zone_compare"
        assert "Prosperidad" in payload["title"]
        assert "Guindalera" in payload["title"]
        assert len(payload["suggestions"]) == 2
        assert len(payload["followups"]) >= 1
    finally:
        session.close()


def test_copilot_can_compare_zone_by_confidence_with_natural_language(monkeypatch) -> None:
    session = make_session()
    try:
        monkeypatch.setattr(
            copilot_service,
            "get_zone_intelligence_v2",
            lambda session, window_days=14: [
                {
                    "zone_label": "Prosperidad",
                    "zone_capture_score": 61.0,
                    "zone_confidence_score": 78.0,
                    "zone_transformation_signal_score": 21.0,
                    "zone_relative_heat_score": 58.0,
                    "zone_liquidity_score": 48.0,
                    "zone_heat_score": 54.0,
                    "events_14d_per_10k_population": 0.5,
                    "geo_point_ratio": 0.82,
                    "resolved_ratio": 0.71,
                    "casafari_raw_in_zone": 12,
                    "official_population": 36961,
                    "ai_brief": "Buena lectura comercial.",
                    "ai_next_step": "Seguir y vigilar.",
                    "executive_summary": "Prosperidad va fuerte.",
                    "recommended_action": "Seguir y vigilar",
                },
                {
                    "zone_label": "Guindalera",
                    "zone_capture_score": 54.0,
                    "zone_confidence_score": 57.0,
                    "zone_transformation_signal_score": 18.0,
                    "zone_relative_heat_score": 52.0,
                    "zone_liquidity_score": 41.0,
                    "zone_heat_score": 49.0,
                    "events_14d_per_10k_population": 0.4,
                    "geo_point_ratio": 0.46,
                    "resolved_ratio": 0.38,
                    "casafari_raw_in_zone": 9,
                    "official_population": 28911,
                    "ai_brief": "Lectura razonable, algo mas fragil.",
                    "ai_next_step": "Validar confianza.",
                    "executive_summary": "Guindalera aguanta.",
                    "recommended_action": "Validar confianza",
                },
            ],
        )

        payload = copilot_service.run_copilot_query(
            session,
            "por que prosperidad es mejor que guindalera en confianza",
        )

        assert payload["intent"] == "zone_compare"
        assert payload["comparison_focus"] == "confidence"
        assert "confianza" in payload["title"].lower()
        assert "geografia" in payload["answer"].lower() or "casafari" in payload["answer"].lower()
    finally:
        session.close()


def test_copilot_can_trigger_implicit_open_map_from_context() -> None:
    session = make_session()
    try:
        payload = copilot_service.run_copilot_query(
            session,
            "abre la seleccion en mapa",
            context={
                "selected_row": {
                    "tipo": "Zona",
                    "item": "Prosperidad",
                    "target_view": "radar",
                    "zone_label": "Prosperidad",
                }
            },
        )

        assert payload["intent"] == "context_action"
        assert payload["auto_action"] == "open_map"
        assert payload["suggestions"][0]["zone_label"] == "Prosperidad"
    finally:
        session.close()


def test_copilot_can_trigger_implicit_execute_action_from_context() -> None:
    session = make_session()
    try:
        payload = copilot_service.run_copilot_query(
            session,
            "ejecuta la accion",
            context={
                "selected_row": {
                    "tipo": "Accion",
                    "item": "Sincronizar Casafari",
                    "target_view": "sync",
                    "action_id": "casafari_sync",
                }
            },
        )

        assert payload["intent"] == "context_action"
        assert payload["auto_action"] == "execute_action"
        assert payload["suggestions"][0]["action_id"] == "casafari_sync"
    finally:
        session.close()


def test_copilot_understands_more_natural_capture_language(monkeypatch) -> None:
    session = make_session()
    try:
        monkeypatch.setattr(
            copilot_service,
            "get_zone_intelligence_v2",
            lambda session, window_days=14: [
                {
                    "zone_label": "Prosperidad",
                    "zone_capture_score": 61.0,
                    "zone_heat_score": 54.0,
                    "zone_relative_heat_score": 59.0,
                    "zone_transformation_signal_score": 20.0,
                    "predicted_absorption_30d_score": 49.0,
                    "ai_brief": "Buena lectura comercial.",
                    "ai_next_step": "Seguir y vigilar.",
                    "executive_summary": "Prosperidad va fuerte.",
                    "recommended_action": "Seguir y vigilar",
                }
            ],
        )

        payload = copilot_service.run_copilot_query(
            session,
            "donde me centrarias ahora mismo para captar",
        )

        assert payload["intent"] == "zone_capture"
        assert payload["suggestions"][0]["item"] == "Prosperidad"
    finally:
        session.close()


def test_copilot_understands_casafari_repeated_phone_language(monkeypatch) -> None:
    session = make_session()
    try:
        monkeypatch.setattr(
            copilot_service,
            "list_casafari_links",
            lambda session, status_filter="all", focus_filter="all", query_text=None, limit=20: [
                {
                    "address_raw": "calle Alcala 10",
                    "ai_brief": "Telefono repetido en varios anuncios.",
                    "ai_next_step": "Revisar antes de enlazar.",
                    "reason_taxonomy": focus_filter,
                }
            ],
        )

        payload = copilot_service.run_copilot_query(
            session,
            "que ves raro en casafari con telefonos repetidos",
        )

        assert payload["intent"] == "casafari_review"
        assert payload["suggestions"][0]["tipo"] == "Casafari"
    finally:
        session.close()


def test_copilot_understands_more_natural_sync_language() -> None:
    session = make_session()
    try:
        payload = copilot_service.run_copilot_query(session, "trae el delta de casafari")

        assert payload["intent"] == "action_sync"
        assert payload["suggestions"][0]["action_id"] == "casafari_sync"
    finally:
        session.close()


def test_copilot_returns_clarification_when_zone_is_ambiguous(monkeypatch) -> None:
    session = make_session()
    try:
        monkeypatch.setattr(
            copilot_service,
            "get_zone_intelligence_v2",
            lambda session, window_days=14: [
                {"zone_label": "Prosperidad", "ai_brief": "A", "ai_next_step": "A"},
                {"zone_label": "Prosperi Norte", "ai_brief": "B", "ai_next_step": "B"},
            ],
        )

        payload = copilot_service.run_copilot_query(session, "que ves en prosperi")

        assert payload["intent"] == "clarification_needed"
        assert len(payload["followups"]) >= 1
        assert payload["understanding"]["clarification_needed"] is True
    finally:
        session.close()


def test_copilot_understanding_extracts_phone_and_portal(monkeypatch) -> None:
    session = make_session()
    try:
        monkeypatch.setattr(
            copilot_service,
            "search_payload",
            lambda session, query, section_filter="all", limit_per_section=5: {
                "summary": {"assets": 0, "listings": 0, "raws": 0, "events": 0, "total": 0},
                "assets": [],
                "listings": [],
                "raws": [],
                "events": [],
                "index_status": {"backend": "fts5", "doc_count": 10},
            },
        )

        payload = copilot_service.run_copilot_query(session, "busca 699111222 en idealista")

        assert payload["understanding"]["entities"]["phone"] == "699111222"
        assert payload["understanding"]["entities"]["portals"] == ["idealista"]
    finally:
        session.close()


def test_copilot_can_compare_opportunities_using_context(monkeypatch) -> None:
    session = make_session()
    try:
        monkeypatch.setattr(
            copilot_service,
            "get_opportunity_queue_v2",
            lambda session, window_days=14, limit=250: [
                {
                    "event_id": 10,
                    "asset_address": "calle clara del rey",
                    "zone_label": "Prosperidad",
                    "portal": "Idealista",
                    "price_eur": 450000,
                    "score": 71.0,
                    "ai_brief": "Muy fuerte.",
                    "ai_next_step": "Abrir en mapa.",
                },
                {
                    "event_id": 11,
                    "asset_address": "calle Lopez de Hoyos",
                    "zone_label": "Prosperidad",
                    "portal": "Fotocasa",
                    "price_eur": 430000,
                    "score": 64.0,
                    "ai_brief": "Interesante.",
                    "ai_next_step": "Comparar.",
                },
            ],
        )

        payload = copilot_service.run_copilot_query(
            session,
            "comparala con la otra",
            context={
                "selected_row": {
                    "target_view": "queue",
                    "event_id": 10,
                    "item": "calle clara del rey",
                    "tipo": "Oportunidad",
                },
                "recent_rows": [
                    {"target_view": "queue", "event_id": 10, "item": "calle clara del rey"},
                    {"target_view": "queue", "event_id": 11, "item": "calle Lopez de Hoyos"},
                ],
            },
        )

        assert payload["intent"] == "opportunity_compare"
        assert len(payload["suggestions"]) == 2
        assert "clara del rey" in payload["title"].lower()
    finally:
        session.close()


def test_copilot_supports_multi_intent_queries(monkeypatch) -> None:
    session = make_session()
    try:
        monkeypatch.setattr(
            copilot_service,
            "get_zone_intelligence_v2",
            lambda session, window_days=14: [
                {
                    "zone_label": "Prosperidad",
                    "zone_capture_score": 61.0,
                    "zone_heat_score": 54.0,
                    "zone_relative_heat_score": 59.0,
                    "zone_transformation_signal_score": 20.0,
                    "predicted_absorption_30d_score": 49.0,
                    "ai_brief": "Buena lectura comercial.",
                    "ai_next_step": "Seguir y vigilar.",
                    "executive_summary": "Prosperidad va fuerte.",
                    "recommended_action": "Seguir y vigilar",
                }
            ],
        )
        monkeypatch.setattr(
            copilot_service,
            "list_casafari_links",
            lambda session, status_filter="all", focus_filter="all", query_text=None, limit=20: [
                {
                    "address_raw": "calle Alcala 10",
                    "ai_brief": "Telefono repetido en varios anuncios.",
                    "ai_next_step": "Revisar antes de enlazar.",
                    "reason_taxonomy": focus_filter,
                }
            ],
        )

        payload = copilot_service.run_copilot_query(
            session,
            "zonas para captar y casafari con telefonos repetidos",
        )

        assert payload["intent"] == "multi_intent"
        assert len(payload["suggestions"]) >= 2
    finally:
        session.close()
