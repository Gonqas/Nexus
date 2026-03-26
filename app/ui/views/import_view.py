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
    QHeaderView,
    QLabel,
    QProgressBar,
    QPushButton,
    QScrollArea,
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
    if value is None:
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
        scroll.setWidget(page)

        layout = QVBoxLayout(page)

        title = QLabel("Importacion baseline")
        title.setStyleSheet("font-size: 22px; font-weight: bold;")
        layout.addWidget(title)

        subtitle = QLabel(
            "Sube el CSV o Excel del baseline desde la propia app. Puedes seleccionar ficheros, "
            "arrastrarlos aqui o dejarlos en la carpeta inbox para importarlos sin pasar por VS Code."
        )
        subtitle.setStyleSheet("color: #666;")
        subtitle.setWordWrap(True)
        layout.addWidget(subtitle)

        inbox_group = QGroupBox("Carpeta inbox")
        inbox_layout = QVBoxLayout(inbox_group)
        self.inbox_path_label = QLabel(str(self.inbox_dir))
        self.inbox_path_label.setWordWrap(True)
        self.inbox_help_label = QLabel(
            "Deja aqui .csv, .xlsx o .xls para importarlos directamente desde la app."
        )
        self.inbox_help_label.setStyleSheet("color: #666;")
        self.inbox_help_label.setWordWrap(True)

        inbox_buttons = QHBoxLayout()
        self.open_inbox_button = QPushButton("Abrir carpeta inbox")
        self.open_inbox_button.clicked.connect(self.open_inbox_folder)
        self.load_inbox_button = QPushButton("Cargar ficheros inbox")
        self.load_inbox_button.clicked.connect(self.load_pending_inbox_files)
        self.import_inbox_button = QPushButton("Importar inbox")
        self.import_inbox_button.clicked.connect(self.import_pending_inbox_files)

        inbox_buttons.addWidget(self.open_inbox_button)
        inbox_buttons.addWidget(self.load_inbox_button)
        inbox_buttons.addWidget(self.import_inbox_button)
        inbox_buttons.addStretch()

        inbox_layout.addWidget(self.inbox_path_label)
        inbox_layout.addWidget(self.inbox_help_label)
        inbox_layout.addLayout(inbox_buttons)
        layout.addWidget(inbox_group)

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
        self.selected_box = DropFilesTextEdit(self.handle_dropped_files)
        self.selected_box.setReadOnly(True)
        self.selected_box.setMinimumHeight(120)
        self.selected_box.setPlaceholderText(
            "Arrastra aqui los CSV/Excel del baseline o usa el selector."
        )
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
        self.history_table.setCornerButtonEnabled(False)
        self.history_table.setMinimumHeight(170)
        self.history_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.history_table.horizontalHeader().setSectionResizeMode(9, QHeaderView.Stretch)

        history_layout.addWidget(self.history_table)
        layout.addWidget(history_group)

        self.log_box = QTextEdit()
        self.log_box.setReadOnly(True)
        self.log_box.setMinimumHeight(140)
        layout.addWidget(self.log_box)

        self.load_history()
        self.refresh_selected_box()

    def append_log(self, text: str) -> None:
        self.log_box.append(text)

    def refresh_selected_box(self) -> None:
        if self.selected_files:
            self.selected_box.setPlainText("\n".join(self.selected_files))
        else:
            self.selected_box.clear()

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
            self.append_log("No habia ficheros nuevos compatibles para anadir")
            return

        self.selected_files.extend(unique_paths)
        self.refresh_selected_box()
        self.append_log(f"Anadidos {len(unique_paths)} fichero(s) baseline")

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
            self.append_log(f"Abrida carpeta inbox: {self.inbox_dir}")
        except Exception as exc:
            self.append_log(f"No se pudo abrir la carpeta inbox: {exc}")

    def load_pending_inbox_files(self) -> None:
        files = list_pending_baseline_files()
        if not files:
            self.append_log("Inbox vacia: no hay baselines pendientes")
            return
        self.add_selected_files(files)
        self.append_log(f"Cargados {len(files)} fichero(s) desde inbox")

    def import_pending_inbox_files(self) -> None:
        files = list_pending_baseline_files()
        if not files:
            self.append_log("Inbox vacia: nada que importar")
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
            self.append_log("No has seleccionado ningun fichero baseline")
            return

        self.select_button.setEnabled(False)
        self.import_button.setEnabled(False)
        self.open_inbox_button.setEnabled(False)
        self.load_inbox_button.setEnabled(False)
        self.import_inbox_button.setEnabled(False)
        self.progress_bar.setRange(0, 0)
        self.progress_label.setText("Iniciando importacion baseline...")
        self.append_log("Iniciando importacion baseline")

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
        self.progress_label.setText("Importacion baseline completada")

        summary = result["summary"]

        self.append_log("Importacion baseline completada")
        self.append_log(
            f"Ficheros OK: {summary['files_success']} | "
            f"Duplicados: {summary['files_skipped_duplicate']} | "
            f"Error: {summary['files_error']}"
        )
        self.append_log(
            f"Filas: {summary['rows_read']} | "
            f"Listings nuevos: {summary['listings_created']} | "
            f"Snapshots: {summary['snapshots_created']}"
        )
        self.append_log(
            f"Casafari resueltos: {summary['casafari_raw_items_resolved']} | "
            f"Eventos creados: {summary['casafari_market_events_created']}"
        )

        for file_result in result["files"]:
            self.append_log(
                f"{file_result.get('file_name')} -> "
                f"{file_result.get('status')} | "
                f"{file_result.get('message')}"
            )

        self.selected_files = []
        self.refresh_selected_box()
        self.load_history()

    def on_failed(self, error_text: str) -> None:
        self.select_button.setEnabled(True)
        self.import_button.setEnabled(True)
        self.open_inbox_button.setEnabled(True)
        self.load_inbox_button.setEnabled(True)
        self.import_inbox_button.setEnabled(True)
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(0)
        self.progress_label.setText("Error en importacion baseline")

        self.append_log(f"Error: {error_text}")
        self.load_history()
