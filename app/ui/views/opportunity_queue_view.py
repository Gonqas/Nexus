from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from core.services.opportunity_queue_detail_service_v2 import get_opportunity_detail_v2
from core.services.opportunity_queue_service_v2 import get_opportunity_queue_v2
from db.session import SessionLocal


def safe_text(value) -> str:
    if value is None:
        return "-"
    return str(value)


class OpportunityQueueView(QWidget):
    def __init__(self) -> None:
        super().__init__()

        self.rows: list[dict] = []

        layout = QVBoxLayout(self)

        header_layout = QHBoxLayout()
        title = QLabel("Cola operativa")
        title.setStyleSheet("font-size: 22px; font-weight: bold;")

        subtitle = QLabel("Eventos priorizados por señal, zona y contexto operativo")
        subtitle.setStyleSheet("color: #666;")

        self.refresh_button = QPushButton("Refrescar")
        self.refresh_button.clicked.connect(self.load_data)

        header_layout.addWidget(title)
        header_layout.addStretch()
        header_layout.addWidget(self.refresh_button)

        layout.addLayout(header_layout)
        layout.addWidget(subtitle)

        splitter = QSplitter()

        self.table = QTableWidget()
        self.table.setColumnCount(10)
        self.table.setHorizontalHeaderLabels(
            [
                "Score",
                "Evento",
                "Zona",
                "Acción zona",
                "Precio nuevo",
                "Precio ant.",
                "Tipo",
                "Portal",
                "Geo",
                "Motivo",
            ]
        )
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        self.table.itemSelectionChanged.connect(self.on_selected)
        splitter.addWidget(self.table)

        detail_widget = QWidget()
        detail_layout = QVBoxLayout(detail_widget)

        self.detail_title = QLabel("Detalle")
        self.detail_title.setStyleSheet("font-size: 20px; font-weight: bold;")
        detail_layout.addWidget(self.detail_title)

        self.summary_group = QGroupBox("Resumen")
        summary_form = QFormLayout(self.summary_group)
        self.lbl_score = QLabel("-")
        self.lbl_reason = QLabel("-")
        self.lbl_reason.setWordWrap(True)
        self.lbl_zone = QLabel("-")
        self.lbl_zone_capture = QLabel("-")
        self.lbl_zone_pressure = QLabel("-")
        self.lbl_zone_confidence = QLabel("-")
        self.lbl_asset = QLabel("-")
        self.lbl_asset.setWordWrap(True)
        self.lbl_geo = QLabel("-")
        self.lbl_price = QLabel("-")

        summary_form.addRow("Score", self.lbl_score)
        summary_form.addRow("Motivo", self.lbl_reason)
        summary_form.addRow("Zona", self.lbl_zone)
        summary_form.addRow("Capture zona", self.lbl_zone_capture)
        summary_form.addRow("Pressure zona", self.lbl_zone_pressure)
        summary_form.addRow("Confidence zona", self.lbl_zone_confidence)
        summary_form.addRow("Activo", self.lbl_asset)
        summary_form.addRow("Geo", self.lbl_geo)
        summary_form.addRow("Precio", self.lbl_price)
        detail_layout.addWidget(self.summary_group)

        splitter.addWidget(detail_widget)
        splitter.setSizes([1100, 650])

        layout.addWidget(splitter)
        self.load_data()

    def load_data(self) -> None:
        with SessionLocal() as session:
            self.rows = get_opportunity_queue_v2(session, window_days=14, limit=150)

        self.table.setRowCount(len(self.rows))

        for row_idx, row in enumerate(self.rows):
            values = [
                safe_text(row["score"]),
                safe_text(row["event_type"]),
                safe_text(row["zone_label"]),
                safe_text(row["zone_recommended_action"]),
                safe_text(row["price_new"]),
                safe_text(row["price_old"]),
                safe_text(row["asset_type"]),
                safe_text(row["portal"]),
                "Sí" if row["has_geo_point"] else "No",
                safe_text(row["reason"]),
            ]

            for col_idx, value in enumerate(values):
                item = QTableWidgetItem(value)
                if col_idx in (0, 4, 5, 8):
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.table.setItem(row_idx, col_idx, item)

        self.table.resizeColumnsToContents()

        if self.rows:
            self.table.selectRow(0)
            self.load_detail(self.rows[0]["event_id"])

    def on_selected(self) -> None:
        items = self.table.selectedItems()
        if not items:
            return
        row_idx = items[0].row()
        if row_idx < 0 or row_idx >= len(self.rows):
            return
        self.load_detail(self.rows[row_idx]["event_id"])

    def load_detail(self, event_id: int) -> None:
        with SessionLocal() as session:
            detail = get_opportunity_detail_v2(session, event_id, window_days=14)

        if not detail.get("found"):
            self.detail_title.setText("Detalle no encontrado")
            return

        row = detail["queue_row"]
        self.detail_title.setText(f"Oportunidad #{event_id}")
        self.lbl_score.setText(safe_text(row["score"]))
        self.lbl_reason.setText(safe_text(row["reason"]))
        self.lbl_zone.setText(
            f"{safe_text(row['zone_label'])} · {safe_text(row['zone_recommended_action'])}"
        )
        self.lbl_zone_capture.setText(safe_text(row["zone_capture_score"]))
        self.lbl_zone_pressure.setText(safe_text(row["zone_pressure_score"]))
        self.lbl_zone_confidence.setText(safe_text(row["zone_confidence_score"]))
        self.lbl_asset.setText(
            f"{safe_text(row['asset_address'])} · {safe_text(row['asset_type'])}"
        )
        self.lbl_geo.setText(
            f"barrio: {safe_text(row['asset_neighborhood'])} | "
            f"distrito: {safe_text(row['asset_district'])} | "
            f"coords: {'Sí' if row['has_geo_point'] else 'No'}"
        )
        self.lbl_price.setText(
            f"nuevo: {safe_text(row['price_new'])} | anterior: {safe_text(row['price_old'])}"
        )