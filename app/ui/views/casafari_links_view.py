from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QGridLayout,
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

from app.workers.casafari_reconcile_worker import CasafariReconcileWorker
from core.services.casafari_links_service import (
    get_casafari_link_stats,
    list_casafari_links,
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

        layout = QVBoxLayout(self)

        title = QLabel("Reconciliación Casafari")
        title.setStyleSheet("font-size: 22px; font-weight: bold;")
        layout.addWidget(title)

        subtitle = QLabel(
            "Aquí puedes auditar cómo se están enlazando los eventos de Casafari "
            "contra listings, assets y market events, con métricas de calidad del dato."
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

        stats_grid.addWidget(self.raw_card, 0, 0)
        stats_grid.addWidget(self.resolved_card, 0, 1)
        stats_grid.addWidget(self.ambiguous_card, 0, 2)
        stats_grid.addWidget(self.unresolved_card, 1, 0)
        stats_grid.addWidget(self.pending_card, 1, 1)
        stats_grid.addWidget(self.events_card, 1, 2)
        layout.addLayout(stats_grid)

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

        self.table = QTableWidget(0, 16)
        self.table.setHorizontalHeaderLabels(
            [
                "Fecha evento",
                "Tipo",
                "Precisión dir",
                "Zona",
                "Dirección",
                "Contacto",
                "Teléfono",
                "Perfil tlf",
                "Portal",
                "Precio",
                "Conf precio",
                "Estado match",
                "Banda match",
                "Razón",
                "Listing",
                "Nota",
            ]
        )
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(14, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(15, QHeaderView.Stretch)
        layout.addWidget(self.table)

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

        self.raw_card.set_value(str(stats["total_raw"]))
        self.resolved_card.set_value(str(stats["resolved"]))
        self.ambiguous_card.set_value(str(stats["ambiguous"]))
        self.unresolved_card.set_value(str(stats["unresolved"]))
        self.pending_card.set_value(str(stats["pending"]))
        self.events_card.set_value(str(stats["market_events_created"]))

    def load_rows(self) -> None:
        status_filter = self.status_filter.currentText()

        with SessionLocal() as session:
            rows = list_casafari_links(
                session,
                status_filter=status_filter,
                limit=300,
            )

        self.table.setRowCount(len(rows))

        for row_idx, row in enumerate(rows):
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
                safe_text(row["listing_label"]),
                safe_text(row["match_note"]),
            ]

            for col_idx, value in enumerate(values):
                self.table.setItem(row_idx, col_idx, QTableWidgetItem(value))

    def refresh_all(self) -> None:
        self.load_stats()
        self.load_rows()

    def start_rerun(self) -> None:
        if self.worker is not None and self.worker.isRunning():
            return

        self.rerun_button.setEnabled(False)
        self.progress_bar.setRange(0, 0)
        self.progress_label.setText("Reconciliando pendientes...")
        self.append_log("→ Reintentando reconciliación Casafari")

        self.worker = CasafariReconcileWorker(limit=5000)
        self.worker.finished_ok.connect(self.on_rerun_ok)
        self.worker.failed.connect(self.on_rerun_failed)
        self.worker.start()

    def on_rerun_ok(self, result: dict) -> None:
        self.rerun_button.setEnabled(True)
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(1)
        self.progress_label.setText("Reconciliación completada")

        self.append_log("✓ Reconciliación completada")
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
        self.progress_label.setText("Error en reconciliación")

        self.append_log(f"✗ Error: {error_text}")
        self.refresh_all()