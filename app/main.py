from PySide6.QtWidgets import QApplication

from db.init_db import init_database

from .ui.theme import build_app_stylesheet
from .ui.main_window import MainWindow


def main() -> None:
    init_database()

    app = QApplication([])
    app.setApplicationName("Nexus Madrid")
    app.setStyleSheet(build_app_stylesheet())
    window = MainWindow()
    window.show()
    app.exec()


if __name__ == "__main__":
    main()
