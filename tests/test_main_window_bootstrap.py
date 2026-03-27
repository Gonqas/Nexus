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
