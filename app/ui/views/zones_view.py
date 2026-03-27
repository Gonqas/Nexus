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

from core.services.zone_detail_service_v2 import get_zone_detail_v2
from core.services.zone_intelligence_service_v2 import get_zone_intelligence_v2
from db.session import SessionLocal


def safe_text(value) -> str:
    if value is None:
        return "-"
    return str(value)


def safe_pct(value) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value) * 100:.1f}%"
    except Exception:
        return str(value)


class ZonesView(QWidget):
    def __init__(self) -> None:
        super().__init__()

        self._has_loaded = False
        self.rows: list[dict] = []

        layout = QVBoxLayout(self)

        header_layout = QHBoxLayout()
        title = QLabel("Zonas")
        title.setStyleSheet("font-size: 22px; font-weight: bold;")

        subtitle = QLabel("Stock, flow, presión, liquidez, confianza y cobertura geográfica por zona")
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
        self.table.setColumnCount(13)
        self.table.setHorizontalHeaderLabels(
            [
                "Zona",
                "Activos",
                "Listings activos",
                "Nuevos",
                "Bajadas",
                "Absorción",
                "Heat",
                "Pressure",
                "Liquidity",
                "Capture",
                "Confidence",
                "Geo",
                "Acción",
            ]
        )
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        self.table.setSortingEnabled(False)
        self.table.itemSelectionChanged.connect(self.on_zone_selected)
        splitter.addWidget(self.table)

        detail_widget = QWidget()
        detail_layout = QVBoxLayout(detail_widget)

        self.detail_title = QLabel("Detalle de zona")
        self.detail_title.setStyleSheet("font-size: 20px; font-weight: bold;")
        detail_layout.addWidget(self.detail_title)

        self.summary_group = QGroupBox("Resumen")
        summary_form = QFormLayout(self.summary_group)
        self.lbl_assets = QLabel("-")
        self.lbl_listings = QLabel("-")
        self.lbl_avg_price = QLabel("-")
        self.lbl_avg_price_m2 = QLabel("-")
        self.lbl_capture = QLabel("-")
        self.lbl_geo = QLabel("-")
        self.lbl_score_expl = QLabel("-")
        self.lbl_score_expl.setWordWrap(True)
        self.lbl_summary = QLabel("-")
        self.lbl_summary.setWordWrap(True)

        summary_form.addRow("Activos", self.lbl_assets)
        summary_form.addRow("Listings activos", self.lbl_listings)
        summary_form.addRow("Precio medio", self.lbl_avg_price)
        summary_form.addRow("€/m² medio", self.lbl_avg_price_m2)
        summary_form.addRow("Score captación", self.lbl_capture)
        summary_form.addRow("Cobertura geo", self.lbl_geo)
        summary_form.addRow("Explicación score", self.lbl_score_expl)
        summary_form.addRow("Lectura ejecutiva", self.lbl_summary)
        detail_layout.addWidget(self.summary_group)

        self.portals_group = QGroupBox("Portales dominantes")
        portals_layout = QVBoxLayout(self.portals_group)
        self.portals_table = QTableWidget()
        self.portals_table.setColumnCount(2)
        self.portals_table.setHorizontalHeaderLabels(["Portal", "Cuenta"])
        self.portals_table.verticalHeader().setVisible(False)
        portals_layout.addWidget(self.portals_table)
        detail_layout.addWidget(self.portals_group)

        self.types_group = QGroupBox("Tipologías dominantes")
        types_layout = QVBoxLayout(self.types_group)
        self.types_table = QTableWidget()
        self.types_table.setColumnCount(2)
        self.types_table.setHorizontalHeaderLabels(["Tipo", "Cuenta"])
        self.types_table.verticalHeader().setVisible(False)
        types_layout.addWidget(self.types_table)
        detail_layout.addWidget(self.types_group)

        self.events_group = QGroupBox("Eventos Casafari recientes")
        events_layout = QVBoxLayout(self.events_group)
        self.events_table = QTableWidget()
        self.events_table.setColumnCount(5)
        self.events_table.setHorizontalHeaderLabels(
            ["Fecha", "Tipo", "Portal", "Precio", "Listing"]
        )
        self.events_table.verticalHeader().setVisible(False)
        events_layout.addWidget(self.events_table)
        detail_layout.addWidget(self.events_group)

        splitter.addWidget(detail_widget)
        splitter.setSizes([1020, 700])

        layout.addWidget(splitter)

    def ensure_loaded(self, *, force: bool = False) -> None:
        if self._has_loaded and not force:
            return
        self.load_data()

    def load_data(self) -> None:
        self._has_loaded = True
        with SessionLocal() as session:
            self.rows = get_zone_intelligence_v2(session, window_days=14)

        self.table.setRowCount(len(self.rows))

        for row_idx, row in enumerate(self.rows):
            values = [
                safe_text(row["zone_label"]),
                safe_text(row["assets_count"]),
                safe_text(row["active_listings_count"]),
                safe_text(row["listing_detected_count"]),
                safe_text(row["price_drop_count"]),
                safe_text(row["absorption_count"]),
                safe_text(row["zone_heat_score"]),
                safe_text(row["zone_pressure_score"]),
                safe_text(row["zone_liquidity_score"]),
                safe_text(row["zone_capture_score"]),
                safe_text(row["zone_confidence_score"]),
                safe_pct(row.get("geo_point_ratio")),
                safe_text(row["recommended_action"]),
            ]

            for col_idx, value in enumerate(values):
                item = QTableWidgetItem(value)
                if col_idx in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11):
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.table.setItem(row_idx, col_idx, item)

        self.table.resizeColumnsToContents()

        if self.rows:
            self.table.selectRow(0)
            self.load_zone_detail(self.rows[0]["zone_label"])

    def on_zone_selected(self) -> None:
        selected_items = self.table.selectedItems()
        if not selected_items:
            return

        row = selected_items[0].row()
        if row < 0 or row >= len(self.rows):
            return

        zone_label = self.rows[row]["zone_label"]
        self.load_zone_detail(zone_label)

    def load_zone_detail(self, zone_label: str) -> None:
        with SessionLocal() as session:
            detail = get_zone_detail_v2(session, zone_label, window_days=14)

        self.detail_title.setText(f"Detalle: {zone_label}")
        self.lbl_assets.setText(safe_text(detail.get("assets_count")))
        self.lbl_listings.setText(safe_text(detail.get("active_listings_count")))
        self.lbl_avg_price.setText(safe_text(detail.get("avg_price_eur")))
        self.lbl_avg_price_m2.setText(safe_text(detail.get("avg_price_m2")))
        self.lbl_capture.setText(safe_text(detail.get("zone_capture_score")))
        self.lbl_geo.setText(
            f"coords: {safe_pct(detail.get('geo_point_ratio'))} | "
            f"barrio: {safe_pct(detail.get('geo_neighborhood_ratio'))}"
        )
        self.lbl_score_expl.setText(safe_text(detail.get("score_explanation")))
        self.lbl_summary.setText(safe_text(detail.get("executive_summary")))

        self._load_counter_table(self.portals_table, detail.get("top_portals", []))
        self._load_counter_table(self.types_table, detail.get("top_types", []))
        self._load_events_table(detail.get("recent_events", []))

    def _load_counter_table(self, table: QTableWidget, rows: list[tuple[str, int]]) -> None:
        table.setRowCount(len(rows))
        for row_idx, (name, count) in enumerate(rows):
            table.setItem(row_idx, 0, QTableWidgetItem(safe_text(name)))
            count_item = QTableWidgetItem(safe_text(count))
            count_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            table.setItem(row_idx, 1, count_item)
        table.resizeColumnsToContents()

    def _load_events_table(self, events: list) -> None:
        self.events_table.setRowCount(len(events))

        for row_idx, event in enumerate(events):
            listing = event.listing
            asset = event.asset or (listing.asset if listing else None)

            values = [
                safe_text(event.event_datetime.date() if event.event_datetime else ""),
                safe_text(event.event_type),
                safe_text(listing.source_portal if listing else None),
                safe_text(event.price_new),
                safe_text(asset.address_raw if asset else (listing.listing_url if listing else None)),
            ]

            for col_idx, value in enumerate(values):
                item = QTableWidgetItem(value)
                if col_idx in (0, 3):
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.events_table.setItem(row_idx, col_idx, item)

        self.events_table.resizeColumnsToContents()
