from PySide6.QtCore import QUrl
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QProgressBar,
    QPushButton,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
    QComboBox,
)

from app.workers.casafari_reconcile_worker import CasafariReconcileWorker
from app.workers.casafari_session_worker import CasafariSessionWorker
from app.workers.casafari_sync_worker import CasafariSyncWorker
from core.services.casafari_sync_service import get_sync_status
from db.session import SessionLocal


def safe_text(value) -> str:
    if value is None or value == "":
        return "-"
    return str(value)


class SyncMetricCard(QGroupBox):
    def __init__(self, title: str) -> None:
        super().__init__(title)
        self.setMinimumHeight(96)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 12, 14, 12)
        self.value_label = QLabel("-")
        self.value_label.setStyleSheet("font-size: 24px; font-weight: bold;")
        layout.addWidget(self.value_label)
        self.detail_label = QLabel("")
        self.detail_label.setWordWrap(True)
        self.detail_label.setStyleSheet("color: #666;")
        layout.addWidget(self.detail_label)
        layout.addStretch()

    def set_content(self, value: str, detail: str) -> None:
        self.value_label.setText(value)
        self.detail_label.setText(detail)


class SyncView(QWidget):
    def __init__(self) -> None:
        super().__init__()

        self.session_worker: CasafariSessionWorker | None = None
        self.worker: CasafariSyncWorker | None = None
        self.reconcile_worker: CasafariReconcileWorker | None = None
        self.session_ready = False

        layout = QVBoxLayout(self)
        layout.setSpacing(14)

        title = QLabel("Consola Casafari")
        title.setStyleSheet("font-size: 22px; font-weight: bold;")
        layout.addWidget(title)

        subtitle = QLabel(
            "Sincroniza, revisa cobertura y reintenta matching desde una sola pantalla. Elige el modo segun la necesidad."
        )
        subtitle.setStyleSheet("color: #666;")
        subtitle.setWordWrap(True)
        layout.addWidget(subtitle)

        controls = QHBoxLayout()
        self.mode_combo = QComboBox()
        self.mode_combo.addItems(["fast", "balanced", "diagnostic"])
        self.mode_combo.setCurrentText("balanced")

        self.session_button = QPushButton("Preparar sesion")
        self.session_button.clicked.connect(self.start_prepare_session)

        self.sync_button = QPushButton("Sincronizar Casafari")
        self.sync_button.clicked.connect(self.start_sync)

        self.reconcile_button = QPushButton("Reconciliar pendientes")
        self.reconcile_button.clicked.connect(self.start_reconcile)

        self.open_debug_button = QPushButton("Abrir debug")
        self.open_debug_button.clicked.connect(self.open_latest_debug_dir)

        self.refresh_button = QPushButton("Refrescar estado")
        self.refresh_button.clicked.connect(self.load_status)

        controls.addWidget(QLabel("Modo:"))
        controls.addWidget(self.mode_combo)
        controls.addWidget(self.session_button)
        controls.addWidget(self.sync_button)
        controls.addWidget(self.reconcile_button)
        controls.addWidget(self.open_debug_button)
        controls.addWidget(self.refresh_button)
        controls.addStretch()
        layout.addLayout(controls)

        metrics = QGridLayout()
        metrics.setHorizontalSpacing(14)
        metrics.setVerticalSpacing(14)
        self.status_card = SyncMetricCard("Estado")
        self.items_card = SyncMetricCard("Items")
        self.extractor_card = SyncMetricCard("Extractor")
        self.coverage_card = SyncMetricCard("Cobertura")
        self.warning_card = SyncMetricCard("Warnings")
        self.mode_card = SyncMetricCard("Ultimo modo")
        self.session_card = SyncMetricCard("Sesion")

        for idx, card in enumerate(
            [
                self.status_card,
                self.items_card,
                self.extractor_card,
                self.coverage_card,
                self.warning_card,
                self.mode_card,
                self.session_card,
            ]
        ):
            metrics.addWidget(card, idx // 3, idx % 3)
        layout.addLayout(metrics)

        self.progress_label = QLabel("Listo")
        layout.addWidget(self.progress_label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)

        self.tabs = QTabWidget()
        layout.addWidget(self.tabs, 1)

        self._build_status_tab()
        self._build_runtime_tab()
        self._build_log_tab()

        self.load_status()

    def _build_status_tab(self) -> None:
        page = QWidget()
        layout = QVBoxLayout(page)
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
        self.lbl_session_ready = QLabel("-")
        self.lbl_session_saved_at = QLabel("-")
        self.lbl_verified_url = QLabel("-")
        self.lbl_verified_url.setWordWrap(True)
        self.lbl_session_file = QLabel("-")
        self.lbl_session_file.setWordWrap(True)

        form.addRow("Ultimo estado", self.lbl_last_status)
        form.addRow("Ultimo inicio", self.lbl_last_started)
        form.addRow("Ultimo fin", self.lbl_last_finished)
        form.addRow("Ultimo from", self.lbl_last_from)
        form.addRow("Ultimo to", self.lbl_last_to)
        form.addRow("Ultimo numero items", self.lbl_last_count)
        form.addRow("Ultimo mensaje", self.lbl_last_message)
        form.addRow("Sesion lista", self.lbl_session_ready)
        form.addRow("Sesion guardada", self.lbl_session_saved_at)
        form.addRow("URL verificada", self.lbl_verified_url)
        form.addRow("Fichero sesion", self.lbl_session_file)
        layout.addWidget(self.status_group)
        layout.addStretch()
        self.tabs.addTab(page, "Estado")

    def _build_runtime_tab(self) -> None:
        page = QWidget()
        layout = QVBoxLayout(page)
        self.runtime_group = QGroupBox("Ultimo intento")
        form = QFormLayout(self.runtime_group)

        self.lbl_target_url = QLabel("-")
        self.lbl_target_url.setWordWrap(True)
        self.lbl_final_url = QLabel("-")
        self.lbl_final_url.setWordWrap(True)
        self.lbl_extractor = QLabel("-")
        self.lbl_pages_seen = QLabel("-")
        self.lbl_candidate_payloads = QLabel("-")
        self.lbl_coverage_gap = QLabel("-")
        self.lbl_debug_dir = QLabel("-")
        self.lbl_debug_dir.setWordWrap(True)
        self.lbl_warnings = QLabel("-")
        self.lbl_warnings.setWordWrap(True)

        form.addRow("URL objetivo", self.lbl_target_url)
        form.addRow("URL final", self.lbl_final_url)
        form.addRow("Extractor", self.lbl_extractor)
        form.addRow("Paginas vistas", self.lbl_pages_seen)
        form.addRow("Payloads", self.lbl_candidate_payloads)
        form.addRow("Gap cobertura", self.lbl_coverage_gap)
        form.addRow("Debug dir", self.lbl_debug_dir)
        form.addRow("Warnings", self.lbl_warnings)
        layout.addWidget(self.runtime_group)
        layout.addStretch()
        self.tabs.addTab(page, "Diagnostico")

    def _build_log_tab(self) -> None:
        page = QWidget()
        layout = QVBoxLayout(page)
        self.log_box = QTextEdit()
        self.log_box.setReadOnly(True)
        layout.addWidget(self.log_box)
        self.tabs.addTab(page, "Log")

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

        self.lbl_target_url.setText(safe_text(status.get("last_target_url")))
        self.lbl_final_url.setText(safe_text(status.get("last_final_url")))
        self.lbl_extractor.setText(safe_text(status.get("last_extractor_used")))
        self.lbl_pages_seen.setText(safe_text(status.get("last_pages_seen")))
        self.lbl_candidate_payloads.setText(
            f"{safe_text(status.get('last_candidate_payload_count'))} / "
            f"{safe_text(status.get('last_captured_payload_count'))} capturados"
        )
        self.lbl_coverage_gap.setText(
            f"{safe_text(status.get('last_coverage_gap'))} de {safe_text(status.get('last_total_expected'))}"
        )
        self.lbl_debug_dir.setText(safe_text(status.get("last_debug_dir")))
        warnings = status.get("last_warnings") or []
        self.lbl_warnings.setText(" | ".join(str(w) for w in warnings[:4]) if warnings else "-")

        self.status_card.set_content(
            safe_text(status["last_status"]),
            safe_text(status["last_finished_at"]),
        )
        self.items_card.set_content(
            safe_text(status["last_item_count"]),
            f"paginas {safe_text(status.get('last_pages_seen'))}",
        )
        self.extractor_card.set_content(
            safe_text(status.get("last_extractor_used")),
            safe_text(status.get("last_final_url")),
        )
        self.coverage_card.set_content(
            safe_text(status.get("last_coverage_gap")),
            f"esperados {safe_text(status.get('last_total_expected'))}",
        )
        self.warning_card.set_content(
            safe_text(status.get("last_warning_count")),
            "warnings del ultimo run",
        )
        self.mode_card.set_content(
            safe_text(status.get("last_sync_mode")),
            safe_text(status.get("last_debug_dir")),
        )
        self.session_ready = bool(status.get("session_ready"))
        self.lbl_session_ready.setText("si" if self.session_ready else "no")
        self.lbl_session_saved_at.setText(safe_text(status.get("session_saved_at")))
        self.lbl_verified_url.setText(safe_text(status.get("verified_history_url")))
        self.lbl_session_file.setText(safe_text(status.get("session_file")))
        self.session_card.set_content(
            "lista" if self.session_ready else "pendiente",
            safe_text(status.get("session_saved_at")),
        )
        self.sync_button.setEnabled(self.session_ready)
        self.session_button.setEnabled(True)

    def open_latest_debug_dir(self) -> None:
        debug_dir = self.lbl_debug_dir.text().strip()
        if not debug_dir or debug_dir == "-":
            self.append_log("x No hay carpeta de debug disponible")
            return
        QDesktopServices.openUrl(QUrl.fromLocalFile(debug_dir))

    def start_prepare_session(self) -> None:
        if self.session_worker is not None and self.session_worker.isRunning():
            return

        self.session_button.setEnabled(False)
        self.sync_button.setEnabled(False)
        self.progress_bar.setRange(0, 0)
        self.progress_label.setText("Preparando sesion Casafari...")
        self.append_log("-> Preparando sesion Casafari")

        self.session_worker = CasafariSessionWorker()
        self.session_worker.progress_changed.connect(self.on_progress)
        self.session_worker.finished_ok.connect(self.on_session_ready)
        self.session_worker.failed.connect(self.on_session_failed)
        self.session_worker.start()

    def start_sync(self) -> None:
        if self.worker is not None and self.worker.isRunning():
            return
        if not self.session_ready:
            self.progress_label.setText("Primero hay que preparar la sesion")
            self.append_log(
                "x No hay sesion Casafari lista. Abro ahora el flujo para prepararla."
            )
            self.start_prepare_session()
            return

        self.session_button.setEnabled(False)
        self.sync_button.setEnabled(False)
        self.progress_bar.setRange(0, 0)
        self.progress_label.setText("Iniciando sincronizacion...")
        self.append_log(f"-> Sync Casafari modo={self.mode_combo.currentText()}")

        self.worker = CasafariSyncWorker(sync_mode=self.mode_combo.currentText())
        self.worker.progress_changed.connect(self.on_progress)
        self.worker.finished_ok.connect(self.on_finished_ok)
        self.worker.failed.connect(self.on_failed)
        self.worker.start()

    def start_reconcile(self) -> None:
        if self.reconcile_worker is not None and self.reconcile_worker.isRunning():
            return

        self.reconcile_button.setEnabled(False)
        self.progress_bar.setRange(0, 0)
        self.progress_label.setText("Reconciliando pendientes...")
        self.append_log("-> Reconciliando pendientes Casafari")

        self.reconcile_worker = CasafariReconcileWorker(limit=5000)
        self.reconcile_worker.finished_ok.connect(self.on_reconcile_ok)
        self.reconcile_worker.failed.connect(self.on_reconcile_failed)
        self.reconcile_worker.start()

    def on_progress(self, message: str, current: int, total: int) -> None:
        self.progress_label.setText(message)
        if total and total > 0:
            self.progress_bar.setRange(0, total)
            self.progress_bar.setValue(min(current, total))
        else:
            self.progress_bar.setRange(0, 0)
        self.append_log(f"* {message} ({current}/{total if total else '?'})")

    def on_session_ready(self, result: dict) -> None:
        self.session_button.setEnabled(True)
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(1)
        self.progress_label.setText("Sesion Casafari lista")
        self.append_log(
            f"ok Sesion guardada | url={safe_text(result.get('verified_history_url'))}"
        )
        self.load_status()

    def on_session_failed(self, error_text: str) -> None:
        self.session_button.setEnabled(True)
        self.sync_button.setEnabled(self.session_ready)
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(0)
        self.progress_label.setText("Error preparando sesion")
        self.append_log(f"x Error sesion: {error_text}")
        self.load_status()

    def on_finished_ok(self, result: dict) -> None:
        self.session_button.setEnabled(True)
        self.sync_button.setEnabled(True)
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(1)
        self.progress_label.setText("Sincronizacion completada")

        self.append_log(
            f"ok Sync completado | modo={safe_text(result.get('sync_mode'))} | "
            f"nuevos={safe_text(result.get('raw_items_created'))} | "
            f"actualizados={safe_text(result.get('raw_items_updated'))}"
        )
        if result.get("warnings"):
            self.append_log("warnings: " + " | ".join(str(w) for w in result["warnings"][:4]))
        self.load_status()

    def on_failed(self, error_text: str) -> None:
        self.session_button.setEnabled(True)
        self.sync_button.setEnabled(True)
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(0)
        self.progress_label.setText("Error en sincronizacion")
        self.append_log(f"x Error sync: {error_text}")
        lowered = (error_text or "").lower()
        if "login" in lowered or "sesión guardada" in lowered or "sesion guardada" in lowered:
            self.append_log("-> Reprepara la sesion desde 'Preparar sesion' y vuelve a sincronizar.")
        self.load_status()

    def on_reconcile_ok(self, result: dict) -> None:
        self.reconcile_button.setEnabled(True)
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(1)
        self.progress_label.setText("Reconciliacion completada")
        self.append_log(
            f"ok Reconciliacion | procesados={safe_text(result.get('raw_items_processed'))} | "
            f"resueltos={safe_text(result.get('raw_items_resolved'))} | "
            f"ambiguos={safe_text(result.get('raw_items_ambiguous'))} | "
            f"unresolved={safe_text(result.get('raw_items_unresolved'))}"
        )
        self.load_status()

    def on_reconcile_failed(self, error_text: str) -> None:
        self.reconcile_button.setEnabled(True)
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(0)
        self.progress_label.setText("Error en reconciliacion")
        self.append_log(f"x Error reconcile: {error_text}")
        self.load_status()
