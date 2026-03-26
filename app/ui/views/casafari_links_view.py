from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QProgressBar,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from app.workers.casafari_reconcile_worker import CasafariReconcileWorker
from core.services.casafari_links_service import (
    get_casafari_link_stats,
    get_casafari_matching_review_summary,
    list_casafari_links,
    save_casafari_match_review,
)
from db.session import SessionLocal


def safe_text(value) -> str:
    if value is None or value == "":
        return "-"
    return str(value)


class StatCard(QGroupBox):
    def __init__(self, title: str) -> None:
        super().__init__(title)
        self.setMinimumHeight(96)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 12, 14, 12)
        self.value_label = QLabel("0")
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


class CasafariLinksView(QWidget):
    def __init__(self) -> None:
        super().__init__()

        self.worker: CasafariReconcileWorker | None = None
        self.rows: list[dict] = []
        self.selected_row_payload: dict | None = None

        layout = QVBoxLayout(self)
        layout.setSpacing(14)

        title = QLabel("Consola de revision Casafari")
        title.setStyleSheet("font-size: 22px; font-weight: bold;")
        layout.addWidget(title)

        subtitle = QLabel(
            "Filtra rapido, revisa solo lo importante y guarda decision manual sin pelearte con una tabla interminable."
        )
        subtitle.setStyleSheet("color: #666;")
        subtitle.setWordWrap(True)
        layout.addWidget(subtitle)

        metrics = QGridLayout()
        metrics.setHorizontalSpacing(14)
        metrics.setVerticalSpacing(14)

        self.raw_card = StatCard("Raw")
        self.resolved_card = StatCard("Resueltos")
        self.unresolved_card = StatCard("Sin resolver")
        self.pending_card = StatCard("Pendientes")
        self.reviews_card = StatCard("Reviews")
        self.precision_card = StatCard("Precision")

        for idx, card in enumerate(
            [
                self.raw_card,
                self.resolved_card,
                self.unresolved_card,
                self.pending_card,
                self.reviews_card,
                self.precision_card,
            ]
        ):
            metrics.addWidget(card, idx // 3, idx % 3)
        layout.addLayout(metrics)

        self.threshold_label = QLabel("Thresholds: sin muestra")
        self.threshold_label.setStyleSheet("color: #666;")
        self.threshold_label.setWordWrap(True)
        layout.addWidget(self.threshold_label)

        controls = QHBoxLayout()
        self.status_filter = QComboBox()
        self.status_filter.addItems(["all", "resolved", "ambiguous", "unresolved", "pending"])
        self.status_filter.currentTextChanged.connect(self.load_rows)

        self.focus_filter = QComboBox()
        self.focus_filter.addItems(
            [
                "all",
                "review_needed",
                "poor_address",
                "repeated_phone",
                "weak_identity",
                "price_conflict",
            ]
        )
        self.focus_filter.currentTextChanged.connect(self.load_rows)

        self.limit_combo = QComboBox()
        self.limit_combo.addItems(["100", "300", "600"])
        self.limit_combo.setCurrentText("300")
        self.limit_combo.currentTextChanged.connect(self.load_rows)

        self.query_input = QLineEdit()
        self.query_input.setPlaceholderText("Buscar por direccion, telefono, portal o reason")
        self.query_input.returnPressed.connect(self.load_rows)

        self.refresh_button = QPushButton("Refrescar")
        self.refresh_button.clicked.connect(self.refresh_all)

        self.rerun_button = QPushButton("Reconciliar pendientes")
        self.rerun_button.clicked.connect(self.start_rerun)

        controls.addWidget(QLabel("Estado:"))
        controls.addWidget(self.status_filter)
        controls.addWidget(QLabel("Foco:"))
        controls.addWidget(self.focus_filter)
        controls.addWidget(QLabel("Limite:"))
        controls.addWidget(self.limit_combo)
        controls.addWidget(self.query_input, 1)
        controls.addWidget(self.refresh_button)
        controls.addWidget(self.rerun_button)
        layout.addLayout(controls)

        self.summary_label = QLabel("Sin datos")
        self.summary_label.setStyleSheet("color: #666;")
        self.summary_label.setWordWrap(True)
        layout.addWidget(self.summary_label)

        self.progress_label = QLabel("Listo")
        layout.addWidget(self.progress_label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)

        splitter = QSplitter()
        layout.addWidget(splitter, 1)

        self.table = QTableWidget(0, 10)
        self.table.setHorizontalHeaderLabels(
            [
                "Fecha",
                "Tipo",
                "Zona",
                "Direccion",
                "Portal",
                "Telefono",
                "Estado",
                "Banda",
                "Reason",
                "Review",
            ]
        )
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setAlternatingRowColors(True)
        self.table.setCornerButtonEnabled(False)
        self.table.verticalHeader().setVisible(False)
        self.table.itemSelectionChanged.connect(self.on_row_selected)
        splitter.addWidget(self.table)

        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)

        self.tabs = QTabWidget()
        right_layout.addWidget(self.tabs)

        self._build_case_tab()
        self._build_review_tab()
        self._build_log_tab()

        splitter.addWidget(right_panel)
        splitter.setSizes([980, 620])

        self.refresh_all()

    def _build_case_tab(self) -> None:
        page = QWidget()
        layout = QVBoxLayout(page)
        self.case_group = QGroupBox("Caso seleccionado")
        form = QFormLayout(self.case_group)

        self.lbl_case = QLabel("Selecciona una fila")
        self.lbl_case.setWordWrap(True)
        self.lbl_status = QLabel("-")
        self.lbl_reason = QLabel("-")
        self.lbl_reason.setWordWrap(True)
        self.lbl_phone = QLabel("-")
        self.lbl_phone.setWordWrap(True)
        self.lbl_price = QLabel("-")
        self.lbl_price.setWordWrap(True)
        self.lbl_listing = QLabel("-")
        self.lbl_listing.setWordWrap(True)
        self.lbl_note = QLabel("-")
        self.lbl_note.setWordWrap(True)
        self.lbl_review = QLabel("-")
        self.lbl_review.setWordWrap(True)

        form.addRow("Caso", self.lbl_case)
        form.addRow("Estado", self.lbl_status)
        form.addRow("Reason", self.lbl_reason)
        form.addRow("Telefono", self.lbl_phone)
        form.addRow("Precio", self.lbl_price)
        form.addRow("Listing", self.lbl_listing)
        form.addRow("Nota", self.lbl_note)
        form.addRow("Ultimo review", self.lbl_review)
        layout.addWidget(self.case_group)
        layout.addStretch()
        self.tabs.addTab(page, "Caso")

    def _build_review_tab(self) -> None:
        page = QWidget()
        layout = QVBoxLayout(page)
        self.review_group = QGroupBox("Review manual")
        form = QFormLayout(self.review_group)

        self.review_label_combo = QComboBox()
        self.review_label_combo.addItems(["match", "no_match", "uncertain"])

        self.review_reviewer_input = QLineEdit()
        self.review_reviewer_input.setPlaceholderText("Tu nombre o alias")

        self.review_reason_input = QTextEdit()
        self.review_reason_input.setMinimumHeight(110)
        self.review_reason_input.setPlaceholderText(
            "Por que este caso es correcto, incorrecto o incierto"
        )

        self.save_review_button = QPushButton("Guardar review")
        self.save_review_button.clicked.connect(self.save_review)

        form.addRow("Label", self.review_label_combo)
        form.addRow("Reviewer", self.review_reviewer_input)
        form.addRow("Razon", self.review_reason_input)
        form.addRow("", self.save_review_button)
        layout.addWidget(self.review_group)
        layout.addStretch()
        self.tabs.addTab(page, "Review")

    def _build_log_tab(self) -> None:
        page = QWidget()
        layout = QVBoxLayout(page)
        self.log_box = QTextEdit()
        self.log_box.setReadOnly(True)
        layout.addWidget(self.log_box)
        self.tabs.addTab(page, "Log")

    def append_log(self, text: str) -> None:
        self.log_box.append(text)

    def load_stats(self) -> None:
        with SessionLocal() as session:
            stats = get_casafari_link_stats(session)
            review_summary = get_casafari_matching_review_summary(session)

        self.raw_card.set_content(str(stats["total_raw"]), f"links={stats['total_links']}")
        self.resolved_card.set_content(str(stats["resolved"]), f"ambiguos={stats['ambiguous']}")
        self.unresolved_card.set_content(str(stats["unresolved"]), "casos por cerrar")
        self.pending_card.set_content(str(stats["pending"]), "todavia sin pasar")

        metrics = review_summary["metrics"]
        self.reviews_card.set_content(str(metrics["reviews_total"]), f"accuracy {metrics['accuracy'] * 100:.1f}%")
        self.precision_card.set_content(f"{metrics['precision'] * 100:.1f}%", f"recall {metrics['recall'] * 100:.1f}%")

        diagnostics = review_summary["threshold_diagnostics"]
        if diagnostics:
            bits = [f"{row['band']}: {row['recommendation']}" for row in diagnostics[:3]]
            self.threshold_label.setText("Thresholds: " + " | ".join(bits))
        else:
            self.threshold_label.setText("Thresholds: sin muestra")

    def load_rows(self) -> None:
        with SessionLocal() as session:
            self.rows = list_casafari_links(
                session,
                status_filter=self.status_filter.currentText(),
                focus_filter=self.focus_filter.currentText(),
                query_text=self.query_input.text(),
                limit=int(self.limit_combo.currentText()),
            )

        self.table.setRowCount(len(self.rows))
        for row_idx, row in enumerate(self.rows):
            values = [
                safe_text(row["event_datetime"]),
                safe_text(row["event_type_guess"]),
                safe_text(row["zone_like_label"] or row["address_precision"]),
                safe_text(row["address_raw"]),
                safe_text(row["portal"]),
                safe_text(row["contact_phone"]),
                safe_text(row["match_status"]),
                safe_text(row["match_confidence_band"]),
                safe_text(row["reason_taxonomy"]),
                safe_text(row["latest_review_label"]),
            ]
            for col_idx, value in enumerate(values):
                item = QTableWidgetItem(value)
                self.table.setItem(row_idx, col_idx, item)

        self.table.resizeColumnsToContents()
        self.summary_label.setText(
            f"{len(self.rows)} casos visibles | estado={self.status_filter.currentText()} | foco={self.focus_filter.currentText()}"
        )

        if self.rows:
            self.table.selectRow(0)
            self.on_row_selected()
        else:
            self.selected_row_payload = None
            self.clear_case()

    def on_row_selected(self) -> None:
        row_idx = self.table.currentRow()
        if row_idx < 0 or row_idx >= len(self.rows):
            self.selected_row_payload = None
            self.clear_case()
            return

        row = self.rows[row_idx]
        self.selected_row_payload = row

        self.lbl_case.setText(
            f"raw={safe_text(row['raw_history_item_id'])} | {safe_text(row['event_type_guess'])} | {safe_text(row['address_raw'])}"
        )
        self.lbl_status.setText(
            f"{safe_text(row['match_status'])} | banda={safe_text(row['match_confidence_band'])} | score={safe_text(row['match_score'])}"
        )
        self.lbl_reason.setText(
            f"{safe_text(row['reason_taxonomy'])} | {safe_text(row['address_precision'])}"
        )
        self.lbl_phone.setText(
            f"{safe_text(row['contact_phone'])} | perfil={safe_text(row['phone_profile'])} | listings={safe_text(row['phone_listing_count'])}"
        )
        self.lbl_price.setText(
            f"actual={safe_text(row['current_price_eur'])} | previa={safe_text(row['previous_price_eur'])} | confianza={safe_text(row['price_confidence'])}"
        )
        self.lbl_listing.setText(
            f"{safe_text(row['listing_label'])} | listing_id={safe_text(row['listing_id'])}"
        )
        self.lbl_note.setText(safe_text(row["match_note"]))

        latest_bits = [
            safe_text(row["latest_review_label"]),
            safe_text(row["latest_review_reviewer"]),
            safe_text(row["latest_review_created_at"]),
        ]
        visible_bits = [bit for bit in latest_bits if bit != "-"]
        self.lbl_review.setText(" | ".join(visible_bits) if visible_bits else "-")

        latest_label = row.get("latest_review_label")
        if latest_label:
            idx = self.review_label_combo.findText(str(latest_label))
            if idx >= 0:
                self.review_label_combo.setCurrentIndex(idx)
        self.review_reason_input.setPlainText(row.get("latest_review_reason") or "")

    def clear_case(self) -> None:
        self.lbl_case.setText("Selecciona una fila")
        self.lbl_status.setText("-")
        self.lbl_reason.setText("-")
        self.lbl_phone.setText("-")
        self.lbl_price.setText("-")
        self.lbl_listing.setText("-")
        self.lbl_note.setText("-")
        self.lbl_review.setText("-")
        self.review_reason_input.clear()

    def refresh_all(self) -> None:
        self.load_stats()
        self.load_rows()

    def save_review(self) -> None:
        row = self.selected_row_payload
        if row is None:
            self.append_log("x Selecciona una fila antes de guardar review")
            return

        with SessionLocal() as session:
            saved = save_casafari_match_review(
                session,
                raw_history_item_id=row.get("raw_history_item_id"),
                listing_id=row.get("listing_id"),
                asset_id=row.get("asset_id"),
                review_label=self.review_label_combo.currentText().strip(),
                review_reason=self.review_reason_input.toPlainText().strip() or None,
                reviewer=self.review_reviewer_input.text().strip() or None,
                predicted_status=row.get("match_status"),
                predicted_score=row.get("match_score"),
            )

        self.append_log(
            f"ok Review guardado | raw={safe_text(row.get('raw_history_item_id'))} | "
            f"label={saved['review_label']} | reviewer={safe_text(saved['reviewer'])}"
        )
        self.refresh_all()

    def start_rerun(self) -> None:
        if self.worker is not None and self.worker.isRunning():
            return

        self.rerun_button.setEnabled(False)
        self.progress_bar.setRange(0, 0)
        self.progress_label.setText("Reconciliando pendientes...")
        self.append_log("-> Reintentando reconciliacion Casafari")

        self.worker = CasafariReconcileWorker(limit=5000)
        self.worker.finished_ok.connect(self.on_rerun_ok)
        self.worker.failed.connect(self.on_rerun_failed)
        self.worker.start()

    def on_rerun_ok(self, result: dict) -> None:
        self.rerun_button.setEnabled(True)
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(1)
        self.progress_label.setText("Reconciliacion completada")
        self.append_log(
            f"ok Reconciliacion | procesados={result['raw_items_processed']} | "
            f"resueltos={result['raw_items_resolved']} | "
            f"ambiguos={result['raw_items_ambiguous']} | "
            f"unresolved={result['raw_items_unresolved']}"
        )
        self.refresh_all()

    def on_rerun_failed(self, error_text: str) -> None:
        self.rerun_button.setEnabled(True)
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(0)
        self.progress_label.setText("Error en reconciliacion")
        self.append_log(f"x Error: {error_text}")
        self.refresh_all()
