from PySide6.QtWidgets import (
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from db.repositories.dashboard_repo import get_dashboard_stats
from db.session import SessionLocal


class StatCard(QGroupBox):
    def __init__(self, title: str, value: str, detail: str = "") -> None:
        super().__init__(title)

        layout = QVBoxLayout(self)

        self.value_label = QLabel(value)
        self.value_label.setStyleSheet("font-size: 28px; font-weight: bold;")
        layout.addWidget(self.value_label)

        self.detail_label = QLabel(detail)
        self.detail_label.setStyleSheet("color: #666;")
        self.detail_label.setWordWrap(True)
        layout.addWidget(self.detail_label)

    def set_value(self, value: str) -> None:
        self.value_label.setText(value)

    def set_detail(self, detail: str) -> None:
        self.detail_label.setText(detail)


def safe_text(value) -> str:
    if value is None or value == "":
        return "-"
    return str(value)


def format_ratio(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "0.0%"
    return f"{(numerator / denominator) * 100:.1f}%"


def format_pct(value: float | None) -> str:
    if value is None:
        return "0.0%"
    return f"{float(value) * 100:.1f}%"


class DashboardView(QWidget):
    def __init__(self) -> None:
        super().__init__()

        self.layout = QVBoxLayout(self)

        header = QHBoxLayout()
        header_title = QVBoxLayout()

        title = QLabel("Dashboard")
        title.setStyleSheet("font-size: 22px; font-weight: bold;")
        header_title.addWidget(title)

        subtitle = QLabel(
            "Conteos, calidad de raw, cobertura geo, breakdown de eventos y salud de sync para no trabajar a ciegas."
        )
        subtitle.setStyleSheet("color: #666;")
        subtitle.setWordWrap(True)
        header_title.addWidget(subtitle)

        self.refresh_button = QPushButton("Refrescar")
        self.refresh_button.clicked.connect(self.refresh)

        header.addLayout(header_title)
        header.addStretch()
        header.addWidget(self.refresh_button)
        self.layout.addLayout(header)

        self.summary_label = QLabel("Sin datos")
        self.summary_label.setStyleSheet("color: #666;")
        self.summary_label.setWordWrap(True)
        self.layout.addWidget(self.summary_label)

        self.grid = QGridLayout()
        self.layout.addLayout(self.grid)

        self.assets_card = StatCard("Activos", "0")
        self.buildings_card = StatCard("Edificios", "0")
        self.contacts_card = StatCard("Contactos", "0")
        self.listings_card = StatCard("Listings", "0")
        self.events_card = StatCard("Eventos", "0")
        self.casafari_raw_card = StatCard("Raw Casafari", "0")
        self.casafari_resolved_card = StatCard("Casafari resueltos", "0")
        self.casafari_ambiguous_card = StatCard("Casafari ambiguos", "0")
        self.casafari_unresolved_card = StatCard("Casafari sin resolver", "0")
        self.casafari_events_card = StatCard("Eventos Casafari", "0")
        self.geo_district_card = StatCard("Geo distrito", "0.0%")
        self.geo_neighborhood_card = StatCard("Geo barrio", "0.0%")
        self.geo_point_card = StatCard("Geo coords", "0.0%")
        self.resolved_ratio_card = StatCard("Ratio resolved", "0.0%")
        self.unresolved_ratio_card = StatCard("Ratio unresolved", "0.0%")
        self.raw_price_quality_card = StatCard("Raw sin precio fiable", "0.0%")
        self.raw_address_quality_card = StatCard("Raw dir pobre", "0.0%")
        self.raw_flow_card = StatCard("Flow raw 7d", "0")

        cards = [
            self.assets_card,
            self.buildings_card,
            self.contacts_card,
            self.listings_card,
            self.events_card,
            self.casafari_raw_card,
            self.casafari_resolved_card,
            self.casafari_ambiguous_card,
            self.casafari_unresolved_card,
            self.casafari_events_card,
            self.geo_district_card,
            self.geo_neighborhood_card,
            self.geo_point_card,
            self.resolved_ratio_card,
            self.unresolved_ratio_card,
            self.raw_price_quality_card,
            self.raw_address_quality_card,
            self.raw_flow_card,
        ]

        for idx, card in enumerate(cards):
            self.grid.addWidget(card, idx // 3, idx % 3)

        detail_layout = QHBoxLayout()
        self.layout.addLayout(detail_layout)

        self.events_group = QGroupBox("Eventos Casafari por tipo")
        events_layout = QVBoxLayout(self.events_group)
        self.events_table = QTableWidget(0, 2)
        self.events_table.setHorizontalHeaderLabels(["Tipo", "Cuenta"])
        events_layout.addWidget(self.events_table)
        detail_layout.addWidget(self.events_group)

        self.zones_group = QGroupBox("Zonas con baja confianza")
        zones_layout = QVBoxLayout(self.zones_group)
        self.zones_table = QTableWidget(0, 5)
        self.zones_table.setHorizontalHeaderLabels(
            ["Zona", "Confidence", "Raw", "Geo", "Acción"]
        )
        zones_layout.addWidget(self.zones_table)
        detail_layout.addWidget(self.zones_group)

        self.sync_group = QGroupBox("Estado del último sync Casafari")
        sync_form = QFormLayout(self.sync_group)
        self.sync_status_label = QLabel("-")
        self.sync_finished_label = QLabel("-")
        self.sync_items_label = QLabel("-")
        self.sync_window_label = QLabel("-")
        self.sync_message_label = QLabel("-")
        self.sync_message_label.setWordWrap(True)

        sync_form.addRow("Estado", self.sync_status_label)
        sync_form.addRow("Último fin", self.sync_finished_label)
        sync_form.addRow("Ítems vistos", self.sync_items_label)
        sync_form.addRow("Ventana", self.sync_window_label)
        sync_form.addRow("Mensaje", self.sync_message_label)
        self.layout.addWidget(self.sync_group)

        self.refresh()

    def refresh(self) -> None:
        with SessionLocal() as session:
            stats = get_dashboard_stats(session)

        assets_total = int(stats["assets"])
        raw_total = int(stats["casafari_raw"])
        district_count = int(stats["assets_with_district"])
        neighborhood_count = int(stats["assets_with_neighborhood"])
        point_count = int(stats["assets_with_geo_point"])
        resolved_count = int(stats["casafari_resolved"])
        unresolved_count = int(stats["casafari_unresolved"])
        poor_price_count = int(stats["raws_without_reliable_price"])
        poor_address_count = int(stats["raws_with_poor_address"])

        self.assets_card.set_value(str(stats["assets"]))
        self.buildings_card.set_value(str(stats["buildings"]))
        self.contacts_card.set_value(str(stats["contacts"]))
        self.listings_card.set_value(str(stats["listings"]))
        self.events_card.set_value(str(stats["events"]))
        self.casafari_raw_card.set_value(str(stats["casafari_raw"]))
        self.casafari_raw_card.set_detail(f"links={safe_text(stats['casafari_links'])}")
        self.casafari_resolved_card.set_value(str(stats["casafari_resolved"]))
        self.casafari_resolved_card.set_detail(f"ratio={format_pct(stats['casafari_resolved_ratio'])}")
        self.casafari_ambiguous_card.set_value(str(stats["casafari_ambiguous"]))
        self.casafari_ambiguous_card.set_detail("requieren revisión")
        self.casafari_unresolved_card.set_value(str(stats["casafari_unresolved"]))
        self.casafari_unresolved_card.set_detail(f"ratio={format_pct(stats['casafari_unresolved_ratio'])}")
        self.casafari_events_card.set_value(str(stats["casafari_events"]))
        self.casafari_events_card.set_detail(
            f"7d={safe_text(stats['casafari_events_7d'])} | 30d={safe_text(stats['casafari_events_30d'])}"
        )

        self.geo_district_card.set_value(format_ratio(district_count, assets_total))
        self.geo_district_card.set_detail(f"{district_count}/{assets_total} activos")

        self.geo_neighborhood_card.set_value(format_ratio(neighborhood_count, assets_total))
        self.geo_neighborhood_card.set_detail(f"{neighborhood_count}/{assets_total} activos")

        self.geo_point_card.set_value(format_ratio(point_count, assets_total))
        self.geo_point_card.set_detail(f"{point_count}/{assets_total} activos")

        self.resolved_ratio_card.set_value(format_pct(stats["casafari_resolved_ratio"]))
        self.resolved_ratio_card.set_detail(f"{resolved_count}/{raw_total} raws")

        self.unresolved_ratio_card.set_value(format_pct(stats["casafari_unresolved_ratio"]))
        self.unresolved_ratio_card.set_detail(f"{unresolved_count}/{raw_total} raws")

        self.raw_price_quality_card.set_value(format_ratio(poor_price_count, raw_total))
        self.raw_price_quality_card.set_detail(f"{poor_price_count}/{raw_total} raws")

        self.raw_address_quality_card.set_value(format_ratio(poor_address_count, raw_total))
        self.raw_address_quality_card.set_detail(f"{poor_address_count}/{raw_total} raws")

        self.raw_flow_card.set_value(str(stats["casafari_raw_7d"]))
        self.raw_flow_card.set_detail(
            f"30d={safe_text(stats['casafari_raw_30d'])} | sync={safe_text(stats['last_sync_item_count'])}"
        )

        self.summary_label.setText(
            f"Matching visible: resolved {format_pct(stats['casafari_resolved_ratio'])}, "
            f"unresolved {format_pct(stats['casafari_unresolved_ratio'])}. "
            f"Calidad raw: precio no fiable {format_ratio(poor_price_count, raw_total)}, "
            f"dirección pobre {format_ratio(poor_address_count, raw_total)}. "
            f"Zonas low confidence: {safe_text(stats['low_confidence_zones_count'])}."
        )

        self._load_events_table(stats.get("event_type_breakdown", []))
        self._load_zones_table(stats.get("low_confidence_zones", []))

        self.sync_status_label.setText(safe_text(stats["last_sync_status"]))
        self.sync_finished_label.setText(safe_text(stats["last_sync_finished_at"]))
        self.sync_items_label.setText(safe_text(stats["last_sync_item_count"]))
        self.sync_window_label.setText(
            f"{safe_text(stats['last_sync_from'])} -> {safe_text(stats['last_sync_to'])}"
        )
        self.sync_message_label.setText(safe_text(stats["last_sync_message"]))

    def _load_events_table(self, rows: list[dict]) -> None:
        self.events_table.setRowCount(len(rows))
        for row_idx, row in enumerate(rows):
            self.events_table.setItem(
                row_idx, 0, QTableWidgetItem(safe_text(row.get("event_type")))
            )
            self.events_table.setItem(
                row_idx, 1, QTableWidgetItem(safe_text(row.get("count")))
            )
        self.events_table.resizeColumnsToContents()

    def _load_zones_table(self, rows: list[dict]) -> None:
        self.zones_table.setRowCount(len(rows))
        for row_idx, row in enumerate(rows):
            values = [
                safe_text(row.get("zone_label")),
                safe_text(row.get("zone_confidence_score")),
                safe_text(row.get("casafari_raw_in_zone")),
                format_pct(row.get("geo_point_ratio")),
                safe_text(row.get("recommended_action")),
            ]
            for col_idx, value in enumerate(values):
                self.zones_table.setItem(row_idx, col_idx, QTableWidgetItem(value))
        self.zones_table.resizeColumnsToContents()
