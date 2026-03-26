from pathlib import Path

from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QProgressBar,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from app.workers.csv_import_worker import CsvImportWorker
from core.services.csv_import_service import list_csv_ingestion_runs
from db.session import SessionLocal


def safe_text(value) -> str:
    if value is None:
        return "-"
    return str(value)


class ImportView(QWidget):
    def __init__(self) -> None:
        super().__init__()

        self.worker: CsvImportWorker | None = None
        self.selected_files: list[str] = []

        layout = QVBoxLayout(self)

        title = QLabel("Importación CSV / Excel")
        title.setStyleSheet("font-size: 22px; font-weight: bold;")
        layout.addWidget(title)

        subtitle = QLabel(
            "Usa el baseline CSV/Excel como fuente maestra enriquecida. Al terminar, la app reintenta "
            "reconciliar eventos Casafari pendientes y puede borrar el fichero físico."
        )
        subtitle.setStyleSheet("color: #666;")
        subtitle.setWordWrap(True)
        layout.addWidget(subtitle)

        controls = QHBoxLayout()
        self.select_button = QPushButton("Seleccionar fichero(s)")
        self.select_button.clicked.connect(self.select_files)

        self.import_button = QPushButton("Importar seleccionados")
        self.import_button.clicked.connect(self.start_import)

        self.refresh_button = QPushButton("Refrescar historial")
        self.refresh_button.clicked.connect(self.load_history)

        self.delete_checkbox = QCheckBox("Borrar fichero tras importar bien")
        self.delete_checkbox.setChecked(True)

        controls.addWidget(self.select_button)
        controls.addWidget(self.import_button)
        controls.addWidget(self.refresh_button)
        controls.addWidget(self.delete_checkbox)
        controls.addStretch()

        layout.addLayout(controls)

        selected_group = QGroupBox("Ficheros seleccionados")
        selected_layout = QVBoxLayout(selected_group)
        self.selected_box = QTextEdit()
        self.selected_box.setReadOnly(True)
        self.selected_box.setMinimumHeight(90)
        selected_layout.addWidget(self.selected_box)
        layout.addWidget(selected_group)

        self.progress_label = QLabel("Listo")
        layout.addWidget(self.progress_label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)

        history_group = QGroupBox("Historial de importaciones baseline")
        history_layout = QVBoxLayout(history_group)

        self.history_table = QTableWidget(0, 10)
        self.history_table.setHorizontalHeaderLabels(
            [
                "Fecha",
                "Archivo",
                "Hash",
                "Estado",
                "Filas",
                "Listings nuevos",
                "Snapshots",
                "Resueltos Casafari",
                "Eventos creados",
                "Mensaje",
            ]
        )
        self.history_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.history_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.history_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.history_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.history_table.horizontalHeader().setSectionResizeMode(9, QHeaderView.Stretch)

        history_layout.addWidget(self.history_table)
        layout.addWidget(history_group)

        self.log_box = QTextEdit()
        self.log_box.setReadOnly(True)
        self.log_box.setMinimumHeight(180)
        layout.addWidget(self.log_box)

        self.load_history()

    def append_log(self, text: str) -> None:
        self.log_box.append(text)

    def select_files(self) -> None:
        files, _ = QFileDialog.getOpenFileNames(
            self,
            "Seleccionar CSV/Excel",
            "",
            "Tabular Files (*.csv *.xlsx *.xls)",
        )
        if not files:
            return

        self.selected_files = files
        self.selected_box.setPlainText("\n".join(files))
        self.append_log(f"→ Seleccionados {len(files)} fichero(s) baseline")

    def load_history(self) -> None:
        with SessionLocal() as session:
            runs = list_csv_ingestion_runs(session, limit=100)

        self.history_table.setRowCount(len(runs))

        for row_idx, run in enumerate(runs):
            values = [
                safe_text(run["started_at"]),
                safe_text(run["file_name"]),
                safe_text(run["file_hash"][:12] if run["file_hash"] else "-"),
                safe_text(run["status"]),
                safe_text(run["rows_read"]),
                safe_text(run["listings_created"]),
                safe_text(run["snapshots_created"]),
                safe_text(run["casafari_raw_items_resolved"]),
                safe_text(run["casafari_market_events_created"]),
                safe_text(run["message"]),
            ]

            for col_idx, value in enumerate(values):
                self.history_table.setItem(row_idx, col_idx, QTableWidgetItem(value))

    def start_import(self) -> None:
        if self.worker is not None and self.worker.isRunning():
            return

        if not self.selected_files:
            self.append_log("✗ No has seleccionado ningún fichero baseline")
            return

        self.select_button.setEnabled(False)
        self.import_button.setEnabled(False)
        self.progress_bar.setRange(0, 0)
        self.progress_label.setText("Iniciando importación baseline...")
        self.append_log("→ Iniciando importación baseline")

        self.worker = CsvImportWorker(
            file_paths=self.selected_files,
            delete_after_success=self.delete_checkbox.isChecked(),
        )
        self.worker.progress_changed.connect(self.on_progress)
        self.worker.finished_ok.connect(self.on_finished_ok)
        self.worker.failed.connect(self.on_failed)
        self.worker.start()

    def on_progress(self, message: str, current: int, total: int) -> None:
        self.progress_label.setText(message)

        if total and total > 0:
            self.progress_bar.setRange(0, total)
            self.progress_bar.setValue(min(current, total))
        else:
            self.progress_bar.setRange(0, 0)

        self.append_log(f"• {message} ({current}/{total if total else '?'})")

    def on_finished_ok(self, result: dict) -> None:
        self.select_button.setEnabled(True)
        self.import_button.setEnabled(True)
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(1)
        self.progress_label.setText("Importación baseline completada")

        summary = result["summary"]

        self.append_log("✓ Importación baseline completada")
        self.append_log(
            f"   Ficheros OK: {summary['files_success']} | "
            f"Duplicados: {summary['files_skipped_duplicate']} | "
            f"Error: {summary['files_error']}"
        )
        self.append_log(
            f"   Filas: {summary['rows_read']} | "
            f"Listings nuevos: {summary['listings_created']} | "
            f"Snapshots: {summary['snapshots_created']}"
        )
        self.append_log(
            f"   Casafari resueltos: {summary['casafari_raw_items_resolved']} | "
            f"Eventos creados: {summary['casafari_market_events_created']}"
        )

        for file_result in result["files"]:
            self.append_log(
                f"   - {file_result.get('file_name')} -> "
                f"{file_result.get('status')} | "
                f"{file_result.get('message')}"
            )

        self.selected_files = []
        self.selected_box.clear()
        self.load_history()

    def on_failed(self, error_text: str) -> None:
        self.select_button.setEnabled(True)
        self.import_button.setEnabled(True)
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(0)
        self.progress_label.setText("Error en importación baseline")

        self.append_log(f"✗ Error: {error_text}")
        self.load_history()
