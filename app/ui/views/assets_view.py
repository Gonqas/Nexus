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

from core.services.comparables_service import get_comparables_payload
from db.repositories.asset_repo import get_assets_with_relations
from db.session import SessionLocal


def safe_text(value: object | None) -> str:
    if value is None:
        return "-"
    return str(value)


def safe_money(value: object | None) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):,.0f} €".replace(",", ".")
    except (TypeError, ValueError):
        return str(value)


def safe_number(value: object | None, decimals: int = 1, suffix: str = "") -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):,.{decimals}f}{suffix}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (TypeError, ValueError):
        return str(value)


class AssetsView(QWidget):
    def __init__(self) -> None:
        super().__init__()

        self.rows = []

        layout = QVBoxLayout(self)

        header_layout = QHBoxLayout()
        title = QLabel("Activos")
        title.setStyleSheet("font-size: 22px; font-weight: bold;")

        subtitle = QLabel("Inventario base con detalle geográfico y comparables por proximidad real")
        subtitle.setStyleSheet("color: #666;")

        self.refresh_button = QPushButton("Refrescar")
        self.refresh_button.clicked.connect(self.load_data)

        header_layout.addWidget(title)
        header_layout.addStretch()
        header_layout.addWidget(self.refresh_button)

        layout.addLayout(header_layout)
        layout.addWidget(subtitle)

        splitter = QSplitter()
        layout.addWidget(splitter)

        self.table = QTableWidget()
        self.table.setColumnCount(10)
        self.table.setHorizontalHeaderLabels(
            [
                "ID",
                "Tipo",
                "Dirección",
                "Barrio",
                "Distrito",
                "m²",
                "Listings",
                "Último precio",
                "€/m²",
                "Portal",
            ]
        )
        self.table.setAlternatingRowColors(True)
        self.table.setSortingEnabled(False)
        self.table.verticalHeader().setVisible(False)
        self.table.itemSelectionChanged.connect(self.on_asset_selected)
        splitter.addWidget(self.table)

        detail_widget = QWidget()
        detail_layout = QVBoxLayout(detail_widget)

        self.detail_title = QLabel("Detalle de activo")
        self.detail_title.setStyleSheet("font-size: 20px; font-weight: bold;")
        detail_layout.addWidget(self.detail_title)

        self.summary_group = QGroupBox("Resumen")
        summary_form = QFormLayout(self.summary_group)
        self.lbl_asset_type = QLabel("-")
        self.lbl_zone = QLabel("-")
        self.lbl_area = QLabel("-")
        self.lbl_price = QLabel("-")
        self.lbl_price_m2 = QLabel("-")
        self.lbl_geo = QLabel("-")
        self.lbl_comps_mode = QLabel("-")
        self.lbl_comps_count = QLabel("-")
        self.lbl_comps_avg_price_m2 = QLabel("-")

        summary_form.addRow("Tipo", self.lbl_asset_type)
        summary_form.addRow("Zona", self.lbl_zone)
        summary_form.addRow("Superficie", self.lbl_area)
        summary_form.addRow("Precio base", self.lbl_price)
        summary_form.addRow("€/m² base", self.lbl_price_m2)
        summary_form.addRow("Geo", self.lbl_geo)
        summary_form.addRow("Modo comparables", self.lbl_comps_mode)
        summary_form.addRow("Nº comparables", self.lbl_comps_count)
        summary_form.addRow("€/m² medio comps", self.lbl_comps_avg_price_m2)
        detail_layout.addWidget(self.summary_group)

        self.comps_group = QGroupBox("Comparables")
        comps_layout = QVBoxLayout(self.comps_group)
        self.comps_table = QTableWidget()
        self.comps_table.setColumnCount(8)
        self.comps_table.setHorizontalHeaderLabels(
            [
                "Asset ID",
                "Zona",
                "Tipo",
                "m²",
                "Precio",
                "€/m²",
                "Distancia",
                "Score",
            ]
        )
        self.comps_table.setAlternatingRowColors(True)
        self.comps_table.verticalHeader().setVisible(False)
        comps_layout.addWidget(self.comps_table)
        detail_layout.addWidget(self.comps_group)

        splitter.addWidget(detail_widget)
        splitter.setSizes([1050, 630])

        self.load_data()

    def load_data(self) -> None:
        with SessionLocal() as session:
            assets = get_assets_with_relations(session, limit=300)

        self.rows = assets
        self.table.setRowCount(len(assets))

        for row_idx, asset in enumerate(assets):
            listings = asset.listings or []
            last_listing = listings[-1] if listings else None

            values = [
                safe_text(asset.id),
                safe_text(asset.asset_type_detail or asset.asset_type_family),
                safe_text(asset.address_raw or asset.address_norm),
                safe_text(asset.neighborhood or (asset.building.neighborhood if asset.building else None)),
                safe_text(asset.district or (asset.building.district if asset.building else None)),
                safe_number(asset.area_m2, decimals=1, suffix=" m²"),
                safe_text(len(listings)),
                safe_money(last_listing.price_eur if last_listing else None),
                safe_money(last_listing.price_per_m2 if last_listing else None),
                safe_text(last_listing.source_portal if last_listing else None),
            ]

            for col_idx, value in enumerate(values):
                item = QTableWidgetItem(value)
                if col_idx in (0, 5, 6):
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.table.setItem(row_idx, col_idx, item)

        self.table.resizeColumnsToContents()

        if assets:
            self.table.selectRow(0)
        else:
            self.clear_detail()

    def on_asset_selected(self) -> None:
        row_idx = self.table.currentRow()
        if row_idx < 0 or row_idx >= len(self.rows):
            self.clear_detail()
            return

        asset = self.rows[row_idx]

        with SessionLocal() as session:
            payload = get_comparables_payload(session, asset.id, limit=8, strict_mode=True)

        subject = payload.get("subject") or {}
        summary = payload.get("summary") or {}
        comparables = payload.get("comparables") or []

        zone_bits = [subject.get("neighborhood"), subject.get("district")]
        zone_text = " · ".join([bit for bit in zone_bits if bit]) or safe_text(subject.get("zone_label"))

        geo_bits = []
        if subject.get("lat") is not None and subject.get("lon") is not None:
            geo_bits.append(f"{float(subject['lat']):.5f}, {float(subject['lon']):.5f}")
        else:
            geo_bits.append("sin coordenadas")

        self.detail_title.setText(f"Detalle de activo #{asset.id}")
        self.lbl_asset_type.setText(safe_text(subject.get("asset_type")))
        self.lbl_zone.setText(zone_text)
        self.lbl_area.setText(safe_number(subject.get("area_m2"), decimals=1, suffix=" m²"))
        self.lbl_price.setText(safe_money(subject.get("price_eur")))
        self.lbl_price_m2.setText(safe_money(subject.get("price_m2")))
        self.lbl_geo.setText(" | ".join(geo_bits))
        self.lbl_comps_mode.setText("Estricto" if summary.get("used_strict_mode") else "Ampliado")
        self.lbl_comps_count.setText(safe_text(summary.get("comparables_count")))
        self.lbl_comps_avg_price_m2.setText(safe_money(summary.get("avg_comparable_price_m2")))

        self.comps_table.setRowCount(len(comparables))
        for comp_row_idx, comp in enumerate(comparables):
            values = [
                safe_text(comp.get("asset_id")),
                safe_text(comp.get("zone_label")),
                safe_text(comp.get("asset_type")),
                safe_number(comp.get("area_m2"), decimals=1, suffix=" m²"),
                safe_money(comp.get("price_eur")),
                safe_money(comp.get("price_m2")),
                safe_number(comp.get("distance_km"), decimals=3, suffix=" km"),
                safe_number(comp.get("score"), decimals=1),
            ]
            for col_idx, value in enumerate(values):
                item = QTableWidgetItem(value)
                if col_idx in (0, 3, 6, 7):
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.comps_table.setItem(comp_row_idx, col_idx, item)

        self.comps_table.resizeColumnsToContents()

    def clear_detail(self) -> None:
        self.detail_title.setText("Detalle de activo")
        self.lbl_asset_type.setText("-")
        self.lbl_zone.setText("-")
        self.lbl_area.setText("-")
        self.lbl_price.setText("-")
        self.lbl_price_m2.setText("-")
        self.lbl_geo.setText("-")
        self.lbl_comps_mode.setText("-")
        self.lbl_comps_count.setText("-")
        self.lbl_comps_avg_price_m2.setText("-")
        self.comps_table.setRowCount(0)