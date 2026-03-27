import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

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
