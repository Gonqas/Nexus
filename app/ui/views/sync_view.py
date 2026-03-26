from PySide6.QtWidgets import (
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QProgressBar,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from app.workers.casafari_sync_worker import CasafariSyncWorker
from core.services.casafari_sync_service import get_sync_status
from db.session import SessionLocal


def safe_text(value) -> str:
    if value is None:
        return "-"
    return str(value)


class SyncView(QWidget):
    def __init__(self) -> None:
        super().__init__()

        self.worker: CasafariSyncWorker | None = None

        layout = QVBoxLayout(self)

        title = QLabel("Sincronización Casafari")
        title.setStyleSheet("font-size: 22px; font-weight: bold;")
        layout.addWidget(title)

        subtitle = QLabel(
            "Pulsa el botón para traer nuevas actualizaciones desde Casafari. "
            "Esta vista ahora también muestra trazabilidad del intento."
        )
        subtitle.setStyleSheet("color: #666;")
        layout.addWidget(subtitle)

        self.status_group = QGroupBox("Estado persistido")
        form = QFormLayout(self.status_group)

        self.lbl_last_status = QLabel("-")
        self.lbl_last_started = QLabel("-")
        self.lbl_last_finished = QLabel("-")
        self.lbl_last_from = QLabel("-")
        self.lbl_last_to = QLabel("-")
        self.lbl_last_count = QLabel("-")
        self.lbl_last_message = QLabel("-")
        self.lbl_last_message.setWordWrap(True)

        form.addRow("Último estado", self.lbl_last_status)
        form.addRow("Último inicio", self.lbl_last_started)
        form.addRow("Último fin", self.lbl_last_finished)
        form.addRow("Último from", self.lbl_last_from)
        form.addRow("Último to", self.lbl_last_to)
        form.addRow("Último nº items", self.lbl_last_count)
        form.addRow("Último mensaje", self.lbl_last_message)

        layout.addWidget(self.status_group)

        self.runtime_group = QGroupBox("Último intento en esta sesión")
        runtime_form = QFormLayout(self.runtime_group)

        self.lbl_target_url = QLabel("-")
        self.lbl_target_url.setWordWrap(True)

        self.lbl_final_url = QLabel("-")
        self.lbl_final_url.setWordWrap(True)

        self.lbl_extractor = QLabel("-")
        self.lbl_pages_seen = QLabel("-")
        self.lbl_candidate_payloads = QLabel("-")
        self.lbl_debug_dir = QLabel("-")
        self.lbl_debug_dir.setWordWrap(True)

        runtime_form.addRow("URL objetivo", self.lbl_target_url)
        runtime_form.addRow("URL final", self.lbl_final_url)
        runtime_form.addRow("Extractor usado", self.lbl_extractor)
        runtime_form.addRow("Páginas vistas", self.lbl_pages_seen)
        runtime_form.addRow("Payloads candidatos", self.lbl_candidate_payloads)
        runtime_form.addRow("Debug dir", self.lbl_debug_dir)

        layout.addWidget(self.runtime_group)

        controls = QHBoxLayout()
        self.sync_button = QPushButton("Sincronizar Casafari")
        self.sync_button.clicked.connect(self.start_sync)

        self.refresh_button = QPushButton("Refrescar estado")
        self.refresh_button.clicked.connect(self.load_status)

        controls.addWidget(self.sync_button)
        controls.addWidget(self.refresh_button)
        controls.addStretch()

        layout.addLayout(controls)

        self.progress_label = QLabel("Listo")
        layout.addWidget(self.progress_label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)

        self.log_box = QTextEdit()
        self.log_box.setReadOnly(True)
        layout.addWidget(self.log_box)

        self.load_status()

    def append_log(self, text: str) -> None:
        self.log_box.append(text)

    def load_status(self) -> None:
        with SessionLocal() as session:
            status = get_sync_status(session)

        self.lbl_last_status.setText(safe_text(status["last_status"]))
        self.lbl_last_started.setText(safe_text(status["last_started_at"]))
        self.lbl_last_finished.setText(safe_text(status["last_finished_at"]))
        self.lbl_last_from.setText(safe_text(status["last_success_from"]))
        self.lbl_last_to.setText(safe_text(status["last_success_to"]))
        self.lbl_last_count.setText(safe_text(status["last_item_count"]))
        self.lbl_last_message.setText(safe_text(status["last_message"]))

    def reset_runtime_labels(self) -> None:
        self.lbl_target_url.setText("-")
        self.lbl_final_url.setText("-")
        self.lbl_extractor.setText("-")
        self.lbl_pages_seen.setText("-")
        self.lbl_candidate_payloads.setText("-")
        self.lbl_debug_dir.setText("-")

    def start_sync(self) -> None:
        if self.worker is not None and self.worker.isRunning():
            return

        self.reset_runtime_labels()
        self.sync_button.setEnabled(False)
        self.progress_bar.setRange(0, 0)
        self.progress_label.setText("Iniciando sincronización...")
        self.append_log("→ Iniciando sincronización Casafari")

        self.worker = CasafariSyncWorker()
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
        self.sync_button.setEnabled(True)
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(1)
        self.progress_label.setText("Sincronización completada")

        self.lbl_target_url.setText(safe_text(result.get("target_url")))
        self.lbl_final_url.setText(safe_text(result.get("final_url")))
        self.lbl_extractor.setText(safe_text(result.get("extractor_used")))
        self.lbl_pages_seen.setText(safe_text(result.get("pages_seen")))
        self.lbl_candidate_payloads.setText(
            f"{safe_text(result.get('candidate_payload_count'))} / "
            f"{safe_text(result.get('captured_payload_count'))} capturados"
        )
        self.lbl_debug_dir.setText(safe_text(result.get("debug_dir")))

        self.append_log("✓ Sincronización completada")
        self.append_log(
            f"   Nuevos: {result['raw_items_created']} | "
            f"Actualizados: {result['raw_items_updated']} | "
            f"Vistos: {result['raw_items_seen']}"
        )
        self.append_log(f"   Extractor: {result.get('extractor_used')}")
        self.append_log(f"   URL final: {result.get('final_url')}")
        self.append_log(f"   Debug dir: {result.get('debug_dir')}")

        self.load_status()

    def on_failed(self, error_text: str) -> None:
        self.sync_button.setEnabled(True)
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(0)
        self.progress_label.setText("Error en sincronización")

        self.append_log(f"✗ Error: {error_text}")
        self.load_status()