from PySide6.QtWidgets import QApplication

from db.init_db import init_database

from .ui.main_window import MainWindow


def main() -> None:
    init_database()

    app = QApplication([])
    window = MainWindow()
    window.show()
    app.exec()


if __name__ == "__main__":
    main()