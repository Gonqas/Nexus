from __future__ import annotations

import os
from pathlib import Path

from PySide6.QtGui import QDragEnterEvent, QDropEvent
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QFileDialog,
    QFrame,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from app.workers.csv_import_worker import CsvImportWorker
from core.services.csv_import_service import list_csv_ingestion_runs
from core.services.import_inbox_service import (
    ensure_baseline_inbox_dir,
    is_supported_baseline_file,
    list_pending_baseline_files,
)
from db.session import SessionLocal


def safe_text(value) -> str:
    if value is None or value == "":
        return "-"
    return str(value)


class DropFilesTextEdit(QTextEdit):
    def __init__(self, on_files_dropped) -> None:
        super().__init__()
        self.on_files_dropped = on_files_dropped
        self.setAcceptDrops(True)

    def dragEnterEvent(self, event: QDragEnterEvent) -> None:
        mime_data = event.mimeData()
        if mime_data.hasUrls():
            event.acceptProposedAction()
            return
        super().dragEnterEvent(event)

    def dropEvent(self, event: QDropEvent) -> None:
        mime_data = event.mimeData()
        if mime_data.hasUrls():
            files = []
            for url in mime_data.urls():
                path = url.toLocalFile()
                if path and is_supported_baseline_file(path):
                    files.append(path)
            if files:
                self.on_files_dropped(files)
                event.acceptProposedAction()
                return
        super().dropEvent(event)


class ImportMetricCard(QGroupBox):
    def __init__(self, title: str) -> None:
        super().__init__(title)
        self.setMinimumHeight(96)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 12, 14, 12)
        self.value_label = QLabel("0")
        self.value_label.setObjectName("MetricValue")
        layout.addWidget(self.value_label)
        self.detail_label = QLabel("")
        self.detail_label.setObjectName("MetricDetail")
        self.detail_label.setWordWrap(True)
        layout.addWidget(self.detail_label)
        layout.addStretch()

    def set_content(self, value: str, detail: str) -> None:
        self.value_label.setText(value)
        self.detail_label.setText(detail)


class ImportView(QWidget):
    def __init__(self) -> None:
        super().__init__()

        self.worker: CsvImportWorker | None = None
        self.selected_files: list[str] = []
        self.inbox_dir = ensure_baseline_inbox_dir()

        root_layout = QVBoxLayout(self)
        root_layout.setContentsMargins(0, 0, 0, 0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        root_layout.addWidget(scroll)

        page = QWidget()
        page.setObjectName("PageScrollContainer")
        scroll.setWidget(page)
        layout = QVBoxLayout(page)
        layout.setContentsMargins(10, 8, 10, 20)
        layout.setSpacing(16)

        title = QLabel("Datos base")
        title.setObjectName("PageTitle")
        layout.addWidget(title)

        subtitle = QLabel(
            "Sube aquí el baseline cuando cambie. Esta pantalla está pensada para reimportar sin fricción, no para un único CSV fijo."
        )
        subtitle.setObjectName("PageSubtitle")
        subtitle.setWordWrap(True)
        layout.addWidget(subtitle)

        cards_row = QHBoxLayout()
        self.selected_card = ImportMetricCard("Seleccionados")
        self.inbox_card = ImportMetricCard("Pendientes inbox")
        self.history_card = ImportMetricCard("Último resultado")
        cards_row.addWidget(self.selected_card, 1)
        cards_row.addWidget(self.inbox_card, 1)
        cards_row.addWidget(self.history_card, 1)
        layout.addLayout(cards_row)

        action_box = QGroupBox("Subir baseline")
        action_layout = QVBoxLayout(action_box)
        action_layout.setSpacing(10)

        buttons_row = QHBoxLayout()
        self.select_button = QPushButton("Seleccionar fichero(s)")
        self.select_button.clicked.connect(self.select_files)
        self.import_button = QPushButton("Importar seleccionados")
        self.import_button.clicked.connect(self.start_import)
        self.delete_checkbox = QCheckBox("Borrar fichero tras importar bien")
        self.delete_checkbox.setChecked(True)
        buttons_row.addWidget(self.select_button)
        buttons_row.addWidget(self.import_button)
        buttons_row.addWidget(self.delete_checkbox)
        buttons_row.addStretch()
        action_layout.addLayout(buttons_row)

        self.selected_box = DropFilesTextEdit(self.handle_dropped_files)
        self.selected_box.setReadOnly(True)
        self.selected_box.setMinimumHeight(120)
        self.selected_box.setPlaceholderText(
            "Arrastra aquí los CSV o Excel del baseline, o usa el selector."
        )
        action_layout.addWidget(self.selected_box)
        layout.addWidget(action_box)

        inbox_box = QGroupBox("Inbox automática")
        inbox_layout = QVBoxLayout(inbox_box)
        self.inbox_path_label = QLabel(str(self.inbox_dir))
        self.inbox_path_label.setWordWrap(True)
        self.inbox_help_label = QLabel(
            "También puedes dejar ficheros en esta carpeta y cargarlos o importarlos desde aquí."
        )
        self.inbox_help_label.setObjectName("MetricDetail")
        self.inbox_help_label.setWordWrap(True)
        inbox_layout.addWidget(self.inbox_path_label)
        inbox_layout.addWidget(self.inbox_help_label)

        inbox_buttons = QHBoxLayout()
        self.open_inbox_button = QPushButton("Abrir inbox")
        self.open_inbox_button.setObjectName("GhostButton")
        self.open_inbox_button.clicked.connect(self.open_inbox_folder)
        self.load_inbox_button = QPushButton("Cargar pendientes")
        self.load_inbox_button.setObjectName("GhostButton")
        self.load_inbox_button.clicked.connect(self.load_pending_inbox_files)
        self.import_inbox_button = QPushButton("Importar inbox")
        self.import_inbox_button.clicked.connect(self.import_pending_inbox_files)
        inbox_buttons.addWidget(self.open_inbox_button)
        inbox_buttons.addWidget(self.load_inbox_button)
        inbox_buttons.addWidget(self.import_inbox_button)
        inbox_buttons.addStretch()
        inbox_layout.addLayout(inbox_buttons)
        layout.addWidget(inbox_box)

        self.progress_label = QLabel("Listo")
        layout.addWidget(self.progress_label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)

        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        self._build_history_tab()
        self._build_log_tab()

        self.load_history()
        self.refresh_selected_box()
        self.refresh_summary_cards()

    def _build_history_tab(self) -> None:
        page = QWidget()
        layout = QVBoxLayout(page)

        self.history_table = QTableWidget(0, 7)
        self.history_table.setHorizontalHeaderLabels(
            [
                "Fecha",
                "Archivo",
                "Estado",
                "Filas",
                "Listings",
                "Eventos",
                "Mensaje",
            ]
        )
        self.history_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.history_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.history_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.history_table.setCornerButtonEnabled(False)
        self.history_table.setMinimumHeight(260)
        layout.addWidget(self.history_table)

        controls = QHBoxLayout()
        self.refresh_button = QPushButton("Actualizar historial")
        self.refresh_button.setObjectName("GhostButton")
        self.refresh_button.clicked.connect(self.load_history)
        controls.addWidget(self.refresh_button)
        controls.addStretch()
        layout.addLayout(controls)

        self.tabs.addTab(page, "Historial")

    def _build_log_tab(self) -> None:
        page = QWidget()
        layout = QVBoxLayout(page)
        self.log_box = QTextEdit()
        self.log_box.setReadOnly(True)
        self.log_box.setMinimumHeight(180)
        layout.addWidget(self.log_box)
        self.tabs.addTab(page, "Log")

    def append_log(self, text: str) -> None:
        self.log_box.append(text)

    def refresh_selected_box(self) -> None:
        if self.selected_files:
            self.selected_box.setPlainText("\n".join(self.selected_files))
        else:
            self.selected_box.clear()

    def refresh_summary_cards(self) -> None:
        pending = list_pending_baseline_files()
        self.selected_card.set_content(
            str(len(self.selected_files)),
            "ficheros listos para importar",
        )
        self.inbox_card.set_content(
            str(len(pending)),
            f"inbox: {self.inbox_dir.name}",
        )

        row_count = self.history_table.rowCount()
        if row_count > 0:
            status_item = self.history_table.item(0, 2)
            file_item = self.history_table.item(0, 1)
            self.history_card.set_content(
                safe_text(status_item.text() if status_item else "-"),
                safe_text(file_item.text() if file_item else "-"),
            )
        else:
            self.history_card.set_content("-", "sin importaciones todavía")

    def add_selected_files(self, files: list[str]) -> None:
        unique_paths = []
        seen = {Path(path).resolve() for path in self.selected_files}
        for path_str in files:
            path = Path(path_str)
            if not is_supported_baseline_file(path):
                continue
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            unique_paths.append(str(resolved))

        if not unique_paths:
            self.append_log("No había ficheros nuevos compatibles para añadir")
            return

        self.selected_files.extend(unique_paths)
        self.refresh_selected_box()
        self.refresh_summary_cards()
        self.append_log(f"Añadidos {len(unique_paths)} fichero(s) baseline")

    def handle_dropped_files(self, files: list[str]) -> None:
        self.add_selected_files(files)
        self.append_log(f"Drag and drop recibido: {len(files)} fichero(s)")

    def select_files(self) -> None:
        files, _ = QFileDialog.getOpenFileNames(
            self,
            "Seleccionar CSV/Excel",
            str(self.inbox_dir),
            "Tabular Files (*.csv *.xlsx *.xls)",
        )
        if not files:
            return
        self.add_selected_files(files)

    def open_inbox_folder(self) -> None:
        ensure_baseline_inbox_dir()
        try:
            os.startfile(str(self.inbox_dir))
            self.append_log(f"Abierta carpeta inbox: {self.inbox_dir}")
        except Exception as exc:
            self.append_log(f"No se pudo abrir la carpeta inbox: {exc}")

    def load_pending_inbox_files(self) -> None:
        files = list_pending_baseline_files()
        if not files:
            self.append_log("Inbox vacía: no hay baselines pendientes")
            return
        self.add_selected_files(files)
        self.append_log(f"Cargados {len(files)} fichero(s) desde inbox")

    def import_pending_inbox_files(self) -> None:
        files = list_pending_baseline_files()
        if not files:
            self.append_log("Inbox vacía: nada que importar")
            return
        self.selected_files = []
        self.add_selected_files(files)
        self.start_import()

    def load_history(self) -> None:
        with SessionLocal() as session:
            runs = list_csv_ingestion_runs(session, limit=100)

        self.history_table.setRowCount(len(runs))
        for row_idx, run in enumerate(runs):
            values = [
                safe_text(run["started_at"]),
                safe_text(run["file_name"]),
                safe_text(run["status"]),
                safe_text(run["rows_read"]),
                safe_text(run["listings_created"]),
                safe_text(run["casafari_market_events_created"]),
                safe_text(run["message"]),
            ]
            for col_idx, value in enumerate(values):
                self.history_table.setItem(row_idx, col_idx, QTableWidgetItem(value))
        self.history_table.resizeColumnsToContents()
        self.refresh_summary_cards()

    def start_import(self) -> None:
        if self.worker is not None and self.worker.isRunning():
            return

        if not self.selected_files:
            self.append_log("No has seleccionado ningún fichero baseline")
            return

        self.select_button.setEnabled(False)
        self.import_button.setEnabled(False)
        self.open_inbox_button.setEnabled(False)
        self.load_inbox_button.setEnabled(False)
        self.import_inbox_button.setEnabled(False)
        self.progress_bar.setRange(0, 0)
        self.progress_label.setText("Iniciando importación baseline...")
        self.append_log("Iniciando importación baseline")

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
        self.append_log(f"- {message} ({current}/{total if total else '?'})")

    def on_finished_ok(self, result: dict) -> None:
        self.select_button.setEnabled(True)
        self.import_button.setEnabled(True)
        self.open_inbox_button.setEnabled(True)
        self.load_inbox_button.setEnabled(True)
        self.import_inbox_button.setEnabled(True)
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(1)
        self.progress_label.setText("Importación baseline completada")

        summary = result["summary"]
        self.append_log("Importación baseline completada")
        self.append_log(
            f"Ficheros OK: {summary['files_success']} | duplicados: {summary['files_skipped_duplicate']} | error: {summary['files_error']}"
        )
        self.append_log(
            f"Filas: {summary['rows_read']} | listings nuevos: {summary['listings_created']} | snapshots: {summary['snapshots_created']}"
        )
        self.append_log(
            f"Casafari resueltos: {summary['casafari_raw_items_resolved']} | eventos creados: {summary['casafari_market_events_created']}"
        )

        for file_result in result["files"]:
            self.append_log(
                f"{file_result.get('file_name')} -> {file_result.get('status')} | {file_result.get('message')}"
            )

        self.selected_files = []
        self.refresh_selected_box()
        self.load_history()
        self.refresh_summary_cards()

    def on_failed(self, error_text: str) -> None:
        self.select_button.setEnabled(True)
        self.import_button.setEnabled(True)
        self.open_inbox_button.setEnabled(True)
        self.load_inbox_button.setEnabled(True)
        self.import_inbox_button.setEnabled(True)
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(0)
        self.progress_label.setText("Error en importación baseline")
        self.append_log(f"Error: {error_text}")
        self.load_history()
        self.refresh_summary_cards()
