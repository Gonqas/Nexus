import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from app.ui.main_window import MainWindow


def test_main_window_loads_first_page_and_keeps_rest_lazy() -> None:
    app = QApplication.instance() or QApplication([])
    window = MainWindow()

    loaded_initial = sum(1 for widget in window.page_widgets if widget is not None)
    assert loaded_initial == 1
    assert window.page_widgets[0] is not None

    window._activate_page(3)
    loaded_after = sum(1 for widget in window.page_widgets if widget is not None)
    assert loaded_after == 2


def test_main_window_opens_copilot_context_in_target_view() -> None:
    app = QApplication.instance() or QApplication([])
    window = MainWindow()

    queue_index = window.page_index_by_key["queue"]
    window._open_copilot_context(
        {
            "target_view": "queue",
            "event_id": 123,
            "zone_label": "Prosperidad",
            "microzone_label": "",
        }
    )

    assert window.stack.currentIndex() == queue_index


def test_main_window_opens_map_context() -> None:
    app = QApplication.instance() or QApplication([])
    window = MainWindow()

    map_index = window.page_index_by_key["map"]
    window._open_map_with_context(
        {
            "zone_label": "Prosperidad",
            "microzone_label": "",
            "event_id": None,
            "window_days": 14,
        }
    )

    assert window.stack.currentIndex() == map_index


def test_main_window_executes_copilot_search_action() -> None:
    app = QApplication.instance() or QApplication([])
    window = MainWindow()

    search_index = window.page_index_by_key["search"]
    window._activate_page(search_index)
    search_view = window.page_widgets[search_index]
    called = {"value": False}

    assert search_view is not None

    def fake_reindex():
        called["value"] = True

    search_view.reindex_fts = fake_reindex  # type: ignore[attr-defined]
    window._execute_copilot_action({"action_id": "search_reindex"})

    assert window.stack.currentIndex() == search_index
    assert called["value"] is True


def test_main_window_launches_sidebar_copilot_prompt() -> None:
    app = QApplication.instance() or QApplication([])
    window = MainWindow()

    search_index = window.page_index_by_key["search"]
    window._launch_copilot_prompt("zonas para captar")

    assert window.stack.currentIndex() == search_index
    search_view = window.page_widgets[search_index]
    assert search_view is not None
    assert search_view.query_input.text() == "zonas para captar"
