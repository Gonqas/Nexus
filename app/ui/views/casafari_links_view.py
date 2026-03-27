from __future__ import annotations

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
    QScrollArea,
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


class CasafariLinksView(QWidget):
    def __init__(self) -> None:
        super().__init__()

        self.worker: CasafariReconcileWorker | None = None
        self.rows: list[dict] = []
        self.selected_row_payload: dict | None = None

        root_layout = QVBoxLayout(self)
        root_layout.setContentsMargins(0, 0, 0, 0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        root_layout.addWidget(scroll)

        page = QWidget()
        page.setObjectName("PageScrollContainer")
        scroll.setWidget(page)
        layout = QVBoxLayout(page)
        layout.setContentsMargins(10, 8, 10, 20)
        layout.setSpacing(16)

        title = QLabel("Casafari")
        title.setObjectName("PageTitle")
        layout.addWidget(title)

        subtitle = QLabel(
            "Revisa solo los casos que merecen atención. El objetivo aquí no es ver todo, sino decidir rápido qué enlace es fiable y cuál no."
        )
        subtitle.setObjectName("PageSubtitle")
        subtitle.setWordWrap(True)
        layout.addWidget(subtitle)

        cards = QGridLayout()
        cards.setHorizontalSpacing(14)
        cards.setVerticalSpacing(14)

        self.raw_card = StatCard("Total raw")
        self.clear_card = StatCard("Casos claros")
        self.pending_card = StatCard("Casos a revisar")
        self.quality_card = StatCard("Precisión")

        for idx, card in enumerate(
            [self.raw_card, self.clear_card, self.pending_card, self.quality_card]
        ):
            cards.addWidget(card, 0, idx)
        layout.addLayout(cards)

        filters_box = QGroupBox("Filtros")
        filters_layout = QVBoxLayout(filters_box)
        filters_layout.setSpacing(10)

        first_row = QHBoxLayout()
        self.query_input = QLineEdit()
        self.query_input.setPlaceholderText("Buscar por dirección, teléfono, portal o motivo")
        self.query_input.returnPressed.connect(self.load_rows)

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

        self.refresh_button = QPushButton("Actualizar")
        self.refresh_button.setObjectName("GhostButton")
        self.refresh_button.clicked.connect(self.refresh_all)

        self.rerun_button = QPushButton("Reconciliar pendientes")
        self.rerun_button.clicked.connect(self.start_rerun)

        first_row.addWidget(QLabel("Buscar"))
        first_row.addWidget(self.query_input, 1)
        first_row.addWidget(QLabel("Estado"))
        first_row.addWidget(self.status_filter)
        first_row.addWidget(QLabel("Foco"))
        first_row.addWidget(self.focus_filter)
        first_row.addWidget(self.refresh_button)
        first_row.addWidget(self.rerun_button)
        filters_layout.addLayout(first_row)

        second_row = QHBoxLayout()
        self.limit_combo = QComboBox()
        self.limit_combo.addItems(["100", "300", "600"])
        self.limit_combo.setCurrentText("300")
        self.limit_combo.currentTextChanged.connect(self.load_rows)

        self.threshold_label = QLabel("Sin muestra suficiente")
        self.threshold_label.setObjectName("MetricDetail")
        self.threshold_label.setWordWrap(True)

        second_row.addWidget(QLabel("Límite"))
        second_row.addWidget(self.limit_combo)
        second_row.addWidget(self.threshold_label, 1)
        filters_layout.addLayout(second_row)
        layout.addWidget(filters_box)

        self.summary_label = QLabel("Sin datos")
        self.summary_label.setObjectName("HeroSummary")
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

        self.table = QTableWidget(0, 7)
        self.table.setHorizontalHeaderLabels(
            ["Fecha", "Señal", "Zona", "Dirección", "Portal", "Estado", "Motivo"]
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
        page.setObjectName("PageScrollContainer")
        layout = QFormLayout(page)
        layout.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
        layout.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        layout.setLabelAlignment(Qt.AlignmentFlag.AlignTop)
        layout.setFormAlignment(Qt.AlignmentFlag.AlignTop)
        layout.setSpacing(12)

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

        layout.addRow("Caso", self.lbl_case)
        layout.addRow("Estado", self.lbl_status)
        layout.addRow("Qué falla o qué encaja", self.lbl_reason)
        layout.addRow("Teléfono", self.lbl_phone)
        layout.addRow("Precio", self.lbl_price)
        layout.addRow("Destino propuesto", self.lbl_listing)
        layout.addRow("Nota del sistema", self.lbl_note)
        layout.addRow("Último review", self.lbl_review)
        self.tabs.addTab(page, "Caso")

    def _build_review_tab(self) -> None:
        page = QWidget()
        page.setObjectName("PageScrollContainer")
        layout = QFormLayout(page)
        layout.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
        layout.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        layout.setLabelAlignment(Qt.AlignmentFlag.AlignTop)
        layout.setFormAlignment(Qt.AlignmentFlag.AlignTop)
        layout.setSpacing(12)

        self.review_label_combo = QComboBox()
        self.review_label_combo.addItems(["match", "no_match", "uncertain"])

        self.review_reviewer_input = QLineEdit()
        self.review_reviewer_input.setPlaceholderText("Tu nombre o alias")

        self.review_reason_input = QTextEdit()
        self.review_reason_input.setMinimumHeight(120)
        self.review_reason_input.setPlaceholderText(
            "Explica en una frase por qué este caso es correcto, incorrecto o dudoso"
        )

        self.save_review_button = QPushButton("Guardar review")
        self.save_review_button.clicked.connect(self.save_review)

        layout.addRow("Decisión", self.review_label_combo)
        layout.addRow("Reviewer", self.review_reviewer_input)
        layout.addRow("Motivo", self.review_reason_input)
        layout.addRow("", self.save_review_button)
        self.tabs.addTab(page, "Review manual")

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
        self.clear_card.set_content(
            str(stats["resolved"]),
            f"ambiguos={stats['ambiguous']}",
        )
        self.pending_card.set_content(
            str(stats["unresolved"]),
            f"pendientes={stats['pending']}",
        )

        metrics = review_summary["metrics"]
        self.quality_card.set_content(
            f"{metrics['precision'] * 100:.1f}%",
            f"recall {metrics['recall'] * 100:.1f}% | reviews {metrics['reviews_total']}",
        )

        diagnostics = review_summary["threshold_diagnostics"]
        if diagnostics:
            bits = [f"{row['band']}: {row['recommendation']}" for row in diagnostics[:2]]
            self.threshold_label.setText("Thresholds: " + " | ".join(bits))
        else:
            self.threshold_label.setText("Thresholds: sin muestra suficiente")

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
                safe_text(row["match_status"]),
                safe_text(row.get("ai_brief") or row["reason_taxonomy"]),
            ]
            for col_idx, value in enumerate(values):
                item = QTableWidgetItem(value)
                if col_idx == 5:
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.table.setItem(row_idx, col_idx, item)

        self.table.resizeColumnsToContents()
        self.summary_label.setText(
            f"{len(self.rows)} casos visibles con estado '{self.status_filter.currentText()}' y foco '{self.focus_filter.currentText()}'."
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
            f"raw {safe_text(row['raw_history_item_id'])} | {safe_text(row['event_type_guess'])}\n{safe_text(row['address_raw'])}"
        )
        self.lbl_status.setText(
            f"{safe_text(row['match_status'])} | banda {safe_text(row['match_confidence_band'])} | score {safe_text(row['match_score'])}"
        )
        self.lbl_reason.setText(
            f"{safe_text(row['reason_taxonomy'])} | dirección {safe_text(row['address_precision'])}"
        )
        self.lbl_reason.setText(safe_text(row.get("ai_summary")))
        self.lbl_phone.setText(
            f"{safe_text(row['contact_phone'])} | perfil {safe_text(row['phone_profile'])}"
        )
        self.lbl_price.setText(
            f"Actual {safe_text(row['current_price_eur'])} | previa {safe_text(row['previous_price_eur'])}"
        )
        self.lbl_listing.setText(
            f"{safe_text(row['listing_label'])} | listing_id {safe_text(row['listing_id'])}"
        )
        self.lbl_note.setText(safe_text(row["match_note"]))

        self.lbl_note.setText(
            f"{safe_text(row.get('ai_next_step'))}\n\nNota tecnica: {safe_text(row['match_note'])}"
        )

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
            f"ok Review guardado | raw={safe_text(row.get('raw_history_item_id'))} | label={saved['review_label']}"
        )
        self.refresh_all()

    def start_rerun(self) -> None:
        if self.worker is not None and self.worker.isRunning():
            return

        self.rerun_button.setEnabled(False)
        self.progress_bar.setRange(0, 0)
        self.progress_label.setText("Reconciliando pendientes...")
        self.append_log("-> Reintentando reconciliación Casafari")

        self.worker = CasafariReconcileWorker(limit=5000)
        self.worker.finished_ok.connect(self.on_rerun_ok)
        self.worker.failed.connect(self.on_rerun_failed)
        self.worker.start()

    def on_rerun_ok(self, result: dict) -> None:
        self.rerun_button.setEnabled(True)
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(1)
        self.progress_label.setText("Reconciliación completada")
        self.append_log(
            f"ok Reconciliación | procesados={result['raw_items_processed']} | resueltos={result['raw_items_resolved']} | ambiguos={result['raw_items_ambiguous']} | unresolved={result['raw_items_unresolved']}"
        )
        self.refresh_all()

    def on_rerun_failed(self, error_text: str) -> None:
        self.rerun_button.setEnabled(True)
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(0)
        self.progress_label.setText("Error en reconciliación")
        self.append_log(f"x Error: {error_text}")
        self.refresh_all()
