from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QProgressBar,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
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
    if value is None:
        return "-"
    return str(value)


class StatCard(QGroupBox):
    def __init__(self, title: str, value: str) -> None:
        super().__init__(title)
        layout = QVBoxLayout(self)

        self.value_label = QLabel(value)
        self.value_label.setStyleSheet("font-size: 26px; font-weight: bold;")
        layout.addWidget(self.value_label)

    def set_value(self, value: str) -> None:
        self.value_label.setText(value)


class CasafariLinksView(QWidget):
    def __init__(self) -> None:
        super().__init__()

        self.worker: CasafariReconcileWorker | None = None
        self.rows: list[dict] = []
        self.selected_row_payload: dict | None = None

        layout = QVBoxLayout(self)

        title = QLabel("Reconciliacion Casafari")
        title.setStyleSheet("font-size: 22px; font-weight: bold;")
        layout.addWidget(title)

        subtitle = QLabel(
            "Auditoria de enlaces Casafari y loop de revision manual para medir precision, "
            "recall y calidad real del matching."
        )
        subtitle.setStyleSheet("color: #666;")
        subtitle.setWordWrap(True)
        layout.addWidget(subtitle)

        stats_grid = QGridLayout()
        self.raw_card = StatCard("Raw Casafari", "0")
        self.resolved_card = StatCard("Resueltos", "0")
        self.ambiguous_card = StatCard("Ambiguos", "0")
        self.unresolved_card = StatCard("No resueltos", "0")
        self.pending_card = StatCard("Pendientes", "0")
        self.events_card = StatCard("Eventos Casafari", "0")
        self.reviews_card = StatCard("Reviews", "0")
        self.precision_card = StatCard("Precision", "0.0%")
        self.recall_card = StatCard("Recall", "0.0%")
        self.accuracy_card = StatCard("Accuracy", "0.0%")

        stats_grid.addWidget(self.raw_card, 0, 0)
        stats_grid.addWidget(self.resolved_card, 0, 1)
        stats_grid.addWidget(self.ambiguous_card, 0, 2)
        stats_grid.addWidget(self.unresolved_card, 1, 0)
        stats_grid.addWidget(self.pending_card, 1, 1)
        stats_grid.addWidget(self.events_card, 1, 2)
        stats_grid.addWidget(self.reviews_card, 2, 0)
        stats_grid.addWidget(self.precision_card, 2, 1)
        stats_grid.addWidget(self.recall_card, 2, 2)
        stats_grid.addWidget(self.accuracy_card, 3, 0)
        layout.addLayout(stats_grid)

        self.threshold_label = QLabel("Thresholds: sin muestra")
        self.threshold_label.setStyleSheet("color: #666;")
        self.threshold_label.setWordWrap(True)
        layout.addWidget(self.threshold_label)

        controls = QHBoxLayout()

        self.status_filter = QComboBox()
        self.status_filter.addItems(
            ["all", "resolved", "ambiguous", "unresolved", "pending"]
        )
        self.status_filter.currentTextChanged.connect(self.load_rows)

        self.refresh_button = QPushButton("Refrescar")
        self.refresh_button.clicked.connect(self.refresh_all)

        self.rerun_button = QPushButton("Reconciliar pendientes")
        self.rerun_button.clicked.connect(self.start_rerun)

        controls.addWidget(QLabel("Filtro estado:"))
        controls.addWidget(self.status_filter)
        controls.addWidget(self.refresh_button)
        controls.addWidget(self.rerun_button)
        controls.addStretch()

        layout.addLayout(controls)

        self.progress_label = QLabel("Listo")
        layout.addWidget(self.progress_label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)

        self.table = QTableWidget(0, 18)
        self.table.setHorizontalHeaderLabels(
            [
                "Fecha evento",
                "Tipo",
                "Precision dir",
                "Zona",
                "Direccion",
                "Contacto",
                "Telefono",
                "Perfil tlf",
                "Portal",
                "Precio",
                "Conf precio",
                "Estado match",
                "Banda match",
                "Razon",
                "Review",
                "Reviewer",
                "Listing",
                "Nota",
            ]
        )
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(16, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(17, QHeaderView.Stretch)
        self.table.itemSelectionChanged.connect(self.on_row_selected)
        layout.addWidget(self.table)

        self.review_group = QGroupBox("Review manual")
        review_layout = QFormLayout(self.review_group)

        self.review_target_label = QLabel("Selecciona una fila para revisar")
        self.review_target_label.setWordWrap(True)

        self.review_latest_label = QLabel("-")
        self.review_latest_label.setWordWrap(True)

        self.review_label_combo = QComboBox()
        self.review_label_combo.addItems(["match", "no_match", "uncertain"])

        self.review_reviewer_input = QLineEdit()
        self.review_reviewer_input.setPlaceholderText("Tu nombre o alias")

        self.review_reason_input = QTextEdit()
        self.review_reason_input.setMinimumHeight(70)
        self.review_reason_input.setPlaceholderText(
            "Por que este caso es correcto, incorrecto o incierto"
        )

        self.save_review_button = QPushButton("Guardar review")
        self.save_review_button.clicked.connect(self.save_review)

        review_layout.addRow("Caso", self.review_target_label)
        review_layout.addRow("Ultimo review", self.review_latest_label)
        review_layout.addRow("Label", self.review_label_combo)
        review_layout.addRow("Reviewer", self.review_reviewer_input)
        review_layout.addRow("Razon", self.review_reason_input)
        review_layout.addRow("", self.save_review_button)
        layout.addWidget(self.review_group)

        self.log_box = QTextEdit()
        self.log_box.setReadOnly(True)
        self.log_box.setMinimumHeight(170)
        layout.addWidget(self.log_box)

        self.refresh_all()

    def append_log(self, text: str) -> None:
        self.log_box.append(text)

    def load_stats(self) -> None:
        with SessionLocal() as session:
            stats = get_casafari_link_stats(session)
            review_summary = get_casafari_matching_review_summary(session)

        self.raw_card.set_value(str(stats["total_raw"]))
        self.resolved_card.set_value(str(stats["resolved"]))
        self.ambiguous_card.set_value(str(stats["ambiguous"]))
        self.unresolved_card.set_value(str(stats["unresolved"]))
        self.pending_card.set_value(str(stats["pending"]))
        self.events_card.set_value(str(stats["market_events_created"]))

        metrics = review_summary["metrics"]
        self.reviews_card.set_value(str(metrics["reviews_total"]))
        self.precision_card.set_value(f"{metrics['precision'] * 100:.1f}%")
        self.recall_card.set_value(f"{metrics['recall'] * 100:.1f}%")
        self.accuracy_card.set_value(f"{metrics['accuracy'] * 100:.1f}%")

        diagnostics = review_summary["threshold_diagnostics"]
        if diagnostics:
            bits = [f"{row['band']}: {row['recommendation']}" for row in diagnostics[:3]]
            self.threshold_label.setText("Thresholds: " + " | ".join(bits))
        else:
            self.threshold_label.setText("Thresholds: sin muestra")

    def load_rows(self) -> None:
        status_filter = self.status_filter.currentText()

        with SessionLocal() as session:
            self.rows = list_casafari_links(
                session,
                status_filter=status_filter,
                limit=300,
            )

        self.table.setRowCount(len(self.rows))

        for row_idx, row in enumerate(self.rows):
            values = [
                safe_text(row["event_datetime"]),
                safe_text(row["event_type_guess"]),
                safe_text(row["address_precision"]),
                safe_text(row["zone_like_label"]),
                safe_text(row["address_raw"]),
                safe_text(row["contact_name"]),
                safe_text(row["contact_phone"]),
                safe_text(
                    f"{row['phone_profile']} ({row['phone_listing_count']})"
                    if row["phone_profile"] != "unknown" or row["phone_listing_count"]
                    else "-"
                ),
                safe_text(row["portal"]),
                safe_text(row["current_price_eur"]),
                safe_text(row["price_confidence"]),
                safe_text(row["match_status"]),
                safe_text(row["match_confidence_band"]),
                safe_text(row["reason_taxonomy"]),
                safe_text(row["latest_review_label"]),
                safe_text(row["latest_review_reviewer"]),
                safe_text(row["listing_label"]),
                safe_text(row["match_note"]),
            ]

            for col_idx, value in enumerate(values):
                self.table.setItem(row_idx, col_idx, QTableWidgetItem(value))

        if self.rows:
            self.table.selectRow(0)
            self.on_row_selected()
        else:
            self.selected_row_payload = None
            self.review_target_label.setText("Selecciona una fila para revisar")
            self.review_latest_label.setText("-")

    def on_row_selected(self) -> None:
        row_idx = self.table.currentRow()
        if row_idx < 0 or row_idx >= len(self.rows):
            self.selected_row_payload = None
            self.review_target_label.setText("Selecciona una fila para revisar")
            self.review_latest_label.setText("-")
            return

        row = self.rows[row_idx]
        self.selected_row_payload = row

        self.review_target_label.setText(
            f"raw={safe_text(row['raw_history_item_id'])} | "
            f"status={safe_text(row['match_status'])} | "
            f"tipo={safe_text(row['event_type_guess'])} | "
            f"dir={safe_text(row['address_raw'])}"
        )

        latest_bits = [
            safe_text(row["latest_review_label"]),
            safe_text(row["latest_review_reviewer"]),
            safe_text(row["latest_review_created_at"]),
        ]
        visible_bits = [bit for bit in latest_bits if bit != "-"]
        self.review_latest_label.setText(" | ".join(visible_bits) if visible_bits else "-")

        latest_label = row.get("latest_review_label")
        if latest_label:
            idx = self.review_label_combo.findText(str(latest_label))
            if idx >= 0:
                self.review_label_combo.setCurrentIndex(idx)

        self.review_reason_input.setPlainText(row.get("latest_review_reason") or "")

    def refresh_all(self) -> None:
        self.load_stats()
        self.load_rows()

    def save_review(self) -> None:
        row = self.selected_row_payload
        if row is None:
            self.append_log("x Selecciona una fila antes de guardar review")
            return

        review_label = self.review_label_combo.currentText().strip()
        reviewer = self.review_reviewer_input.text().strip() or None
        review_reason = self.review_reason_input.toPlainText().strip() or None

        with SessionLocal() as session:
            saved = save_casafari_match_review(
                session,
                raw_history_item_id=row.get("raw_history_item_id"),
                listing_id=row.get("listing_id"),
                asset_id=row.get("asset_id"),
                review_label=review_label,
                review_reason=review_reason,
                reviewer=reviewer,
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

        self.append_log("ok Reconciliacion completada")
        self.append_log(
            f"   Procesados: {result['raw_items_processed']} | "
            f"Resueltos: {result['raw_items_resolved']} | "
            f"Ambiguos: {result['raw_items_ambiguous']} | "
            f"No resueltos: {result['raw_items_unresolved']}"
        )
        self.append_log(
            f"   Market events creados: {result['market_events_created']}"
        )

        self.refresh_all()

    def on_rerun_failed(self, error_text: str) -> None:
        self.rerun_button.setEnabled(True)
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(0)
        self.progress_label.setText("Error en reconciliacion")

        self.append_log(f"x Error: {error_text}")
        self.refresh_all()
