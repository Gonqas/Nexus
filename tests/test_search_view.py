import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

import app.ui.views.search_view as search_view_module
from app.ui.views.search_view import SearchView


def test_search_view_starts_in_copilot_mode() -> None:
    app = QApplication.instance() or QApplication([])
    view = SearchView()

    assert view.classic_results_box.isHidden() is True
    assert view.copilot_button.text() == "Preguntar"
    assert view.classic_toggle_button.text() == "Mostrar busqueda clasica"


def test_search_view_can_toggle_classic_results() -> None:
    app = QApplication.instance() or QApplication([])
    view = SearchView()

    view.show_classic_results(True)
    assert view.classic_results_box.isHidden() is False
    assert view.classic_toggle_button.text() == "Ocultar busqueda clasica"

    view.show_classic_results(False)
    assert view.classic_results_box.isHidden() is True
    assert view.classic_toggle_button.text() == "Mostrar busqueda clasica"


def test_search_view_builds_context_from_selected_row() -> None:
    app = QApplication.instance() or QApplication([])
    view = SearchView()
    view.last_copilot_payload = {
        "intent": "zone_capture",
        "suggestions": [
            {"zone_label": "Prosperidad"},
            {"zone_label": "Guindalera"},
        ],
    }
    view.selected_copilot_row = {
        "target_view": "radar",
        "zone_label": "Prosperidad",
        "tipo": "Zona",
        "item": "Prosperidad",
    }

    context = view._build_copilot_context()

    assert context["selected_row"]["zone_label"] == "Prosperidad"
    assert context["recent_zone_labels"] == ["Prosperidad", "Guindalera"]


def test_search_view_renders_chat_history_and_followups(monkeypatch) -> None:
    app = QApplication.instance() or QApplication([])
    view = SearchView()

    monkeypatch.setattr(
        search_view_module,
        "run_copilot_query",
        lambda session, query, default_limit=10, context=None: {
            "query": query,
            "intent": "zone_capture",
            "title": "Zonas para captar",
            "answer": "Prosperidad y Guindalera salen arriba.",
            "next_step": "Compara las dos mejores.",
            "suggestions": [
                {
                    "tipo": "Zona",
                    "item": "Prosperidad",
                    "por_que": "Buena lectura comercial.",
                    "accion": "Seguir y vigilar",
                    "target_view": "radar",
                    "zone_label": "Prosperidad",
                }
            ],
            "followups": ["explicamela", "comparala con guindalera", "abre la seleccion en mapa"],
            "search_payload": None,
        },
    )

    view.query_input.setText("zonas para captar")
    view.run_copilot()

    assert "zonas para captar" in view.history_label.text().lower()
    assert view.followup_buttons[0].isHidden() is False
    assert view.followup_buttons[0].text() == "explicamela"


def test_search_view_auto_opens_map_when_payload_requests_it(monkeypatch) -> None:
    app = QApplication.instance() or QApplication([])
    view = SearchView()
    emitted = {"value": None}

    view.open_map_requested.connect(lambda payload: emitted.__setitem__("value", payload))

    monkeypatch.setattr(
        search_view_module,
        "run_copilot_query",
        lambda session, query, default_limit=10, context=None: {
            "query": query,
            "intent": "context_action",
            "title": "Abrir en mapa",
            "answer": "Abro el mapa.",
            "next_step": "Validar la zona.",
            "suggestions": [
                {
                    "tipo": "Zona",
                    "item": "Prosperidad",
                    "por_que": "Buena lectura comercial.",
                    "accion": "Abrir en mapa",
                    "target_view": "radar",
                    "zone_label": "Prosperidad",
                }
            ],
            "followups": [],
            "auto_action": "open_map",
            "search_payload": None,
        },
    )

    view.query_input.setText("abre la seleccion en mapa")
    view.run_copilot()

    assert emitted["value"] is not None
    assert emitted["value"]["zone_label"] == "Prosperidad"


def test_search_view_shows_understanding_line(monkeypatch) -> None:
    app = QApplication.instance() or QApplication([])
    view = SearchView()

    monkeypatch.setattr(
        search_view_module,
        "run_copilot_query",
        lambda session, query, default_limit=10, context=None: {
            "query": query,
            "intent": "zone_capture",
            "title": "Zonas para captar",
            "answer": "Prosperidad sale arriba.",
            "next_step": "Abrir en mapa.",
            "suggestions": [],
            "followups": [],
            "understanding": {
                "understanding_text": "intencion zone_capture | zona Prosperidad",
                "confidence": "high",
            },
            "search_payload": None,
        },
    )

    view.query_input.setText("donde me centrarias para captar")
    view.run_copilot()

    assert "intencion zone_capture" in view.understanding_label.text()


def test_search_view_context_includes_recent_rows_and_questions() -> None:
    app = QApplication.instance() or QApplication([])
    view = SearchView()
    view.last_copilot_payload = {
        "intent": "opportunities",
        "suggestions": [
            {"event_id": 10, "item": "calle clara del rey", "target_view": "queue"},
            {"event_id": 11, "item": "calle Lopez de Hoyos", "target_view": "queue"},
        ],
    }
    view.chat_history = [
        {
            "question": "zonas para captar",
            "title": "Zonas para captar",
            "answer": "Prosperidad sale arriba.",
            "intent": "zone_capture",
            "suggestions": [{"zone_label": "Prosperidad", "item": "Prosperidad", "target_view": "radar"}],
        },
        {
            "question": "oportunidades con entrada nueva",
            "title": "Oportunidades",
            "answer": "Hay dos prioritarias.",
            "intent": "opportunities",
            "suggestions": [{"event_id": 10, "item": "calle clara del rey", "target_view": "queue"}],
        },
    ]
    view.selected_copilot_row = {"event_id": 10, "item": "calle clara del rey", "target_view": "queue"}

    context = view._build_copilot_context()

    assert len(context["recent_rows"]) >= 2
    assert context["recent_questions"][0] == "oportunidades con entrada nueva"
