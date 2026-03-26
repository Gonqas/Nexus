from PySide6.QtWidgets import (
    QFrame,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QScrollArea,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from db.repositories.dashboard_repo import get_dashboard_stats
from db.session import SessionLocal


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


class DashboardMetricCard(QWidget):
    def __init__(self, title: str, value: str, detail: str = "") -> None:
        super().__init__()
        self.setObjectName("MetricCard")
        self.setMinimumHeight(112)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 16, 18, 16)
        layout.setSpacing(6)

        self.title_label = QLabel(title)
        self.title_label.setObjectName("MetricTitle")
        layout.addWidget(self.title_label)

        self.value_label = QLabel(value)
        self.value_label.setObjectName("MetricValue")
        layout.addWidget(self.value_label)

        self.detail_label = QLabel(detail)
        self.detail_label.setObjectName("MetricDetail")
        self.detail_label.setWordWrap(True)
        layout.addWidget(self.detail_label)

    def set_value(self, value: str) -> None:
        self.value_label.setText(value)

    def set_detail(self, detail: str) -> None:
        self.detail_label.setText(detail)


class DashboardTablePanel(QGroupBox):
    def __init__(self, title: str, columns: list[str]) -> None:
        super().__init__(title)
        self.setObjectName("DataPanel")

        layout = QVBoxLayout(self)
        self.table = QTableWidget(0, len(columns))
        self.table.setHorizontalHeaderLabels(columns)
        self.table.verticalHeader().setVisible(False)
        self.table.setAlternatingRowColors(True)
        self.table.setCornerButtonEnabled(False)
        self.table.setMinimumHeight(220)
        layout.addWidget(self.table)

    def load_rows(self, rows: list[list[str]]) -> None:
        self.table.setRowCount(len(rows))
        for row_idx, values in enumerate(rows):
            for col_idx, value in enumerate(values):
                self.table.setItem(row_idx, col_idx, QTableWidgetItem(value))
        self.table.resizeColumnsToContents()


class DashboardView(QWidget):
    def __init__(self) -> None:
        super().__init__()

        root_layout = QVBoxLayout(self)
        root_layout.setContentsMargins(0, 0, 0, 0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        root_layout.addWidget(scroll)

        container = QWidget()
        container.setObjectName("PageScrollContainer")
        scroll.setWidget(container)

        self.layout = QVBoxLayout(container)
        self.layout.setContentsMargins(8, 8, 8, 24)
        self.layout.setSpacing(18)

        hero = QWidget()
        hero.setObjectName("HeroPanel")
        hero_layout = QHBoxLayout(hero)
        hero_layout.setContentsMargins(24, 24, 24, 24)
        hero_layout.setSpacing(18)

        hero_copy = QVBoxLayout()
        self.kicker_label = QLabel("North star operativo")
        self.kicker_label.setObjectName("HeroKicker")
        hero_copy.addWidget(self.kicker_label)

        self.title_label = QLabel("Dashboard vivo del nucleo")
        self.title_label.setObjectName("PageTitle")
        hero_copy.addWidget(self.title_label)

        self.subtitle_label = QLabel(
            "Matching, cobertura geo, transformacion y prediccion territorial en una lectura compacta."
        )
        self.subtitle_label.setObjectName("PageSubtitle")
        self.subtitle_label.setWordWrap(True)
        hero_copy.addWidget(self.subtitle_label)

        self.summary_label = QLabel("Sin datos")
        self.summary_label.setObjectName("HeroSummary")
        self.summary_label.setWordWrap(True)
        hero_copy.addWidget(self.summary_label)
        hero_copy.addStretch()

        hero_actions = QVBoxLayout()
        hero_actions.addStretch()
        self.refresh_button = QPushButton("Refrescar dashboard")
        self.refresh_button.clicked.connect(self.refresh)
        hero_actions.addWidget(self.refresh_button)

        hero_layout.addLayout(hero_copy, 1)
        hero_layout.addLayout(hero_actions)
        self.layout.addWidget(hero)

        self.layout.addWidget(self._section_label("Visión general"))
        overview_grid = QGridLayout()
        overview_grid.setHorizontalSpacing(14)
        overview_grid.setVerticalSpacing(14)

        self.assets_card = DashboardMetricCard("Activos", "0")
        self.buildings_card = DashboardMetricCard("Edificios", "0")
        self.contacts_card = DashboardMetricCard("Contactos", "0")
        self.listings_card = DashboardMetricCard("Listings", "0")
        self.events_card = DashboardMetricCard("Eventos", "0")
        self.casafari_raw_card = DashboardMetricCard("Raw Casafari", "0")
        self.casafari_events_card = DashboardMetricCard("Eventos Casafari", "0")
        self.raw_flow_card = DashboardMetricCard("Flow raw 7d", "0")

        overview_cards = [
            self.assets_card,
            self.buildings_card,
            self.contacts_card,
            self.listings_card,
            self.events_card,
            self.casafari_raw_card,
            self.casafari_events_card,
            self.raw_flow_card,
        ]
        for idx, card in enumerate(overview_cards):
            overview_grid.addWidget(card, idx // 4, idx % 4)
        self.layout.addLayout(overview_grid)

        self.layout.addWidget(self._section_label("Matching y calidad"))
        matching_grid = QGridLayout()
        matching_grid.setHorizontalSpacing(14)
        matching_grid.setVerticalSpacing(14)

        self.casafari_resolved_card = DashboardMetricCard("Casafari resueltos", "0")
        self.casafari_ambiguous_card = DashboardMetricCard("Casafari ambiguos", "0")
        self.casafari_unresolved_card = DashboardMetricCard("Casafari sin resolver", "0")
        self.resolved_ratio_card = DashboardMetricCard("Ratio resolved", "0.0%")
        self.unresolved_ratio_card = DashboardMetricCard("Ratio unresolved", "0.0%")
        self.raw_price_quality_card = DashboardMetricCard("Raw sin precio fiable", "0.0%")
        self.raw_address_quality_card = DashboardMetricCard("Raw direccion pobre", "0.0%")

        matching_cards = [
            self.casafari_resolved_card,
            self.casafari_ambiguous_card,
            self.casafari_unresolved_card,
            self.resolved_ratio_card,
            self.unresolved_ratio_card,
            self.raw_price_quality_card,
            self.raw_address_quality_card,
        ]
        for idx, card in enumerate(matching_cards):
            matching_grid.addWidget(card, idx // 4, idx % 4)
        self.layout.addLayout(matching_grid)

        self.layout.addWidget(self._section_label("Geografia, transformacion y prediccion"))
        intelligence_grid = QGridLayout()
        intelligence_grid.setHorizontalSpacing(14)
        intelligence_grid.setVerticalSpacing(14)

        self.geo_district_card = DashboardMetricCard("Geo distrito", "0.0%")
        self.geo_neighborhood_card = DashboardMetricCard("Geo barrio", "0.0%")
        self.geo_point_card = DashboardMetricCard("Geo coords", "0.0%")
        self.change_use_card = DashboardMetricCard("Cambios uso 24m", "0")
        self.closed_locales_card = DashboardMetricCard("Locales cerrados", "0")
        self.vut_units_card = DashboardMetricCard("VUT", "0")
        self.transform_zones_card = DashboardMetricCard("Zonas transformacion", "0")
        self.predictive_zones_card = DashboardMetricCard("Prediccion 30d", "0")

        intelligence_cards = [
            self.geo_district_card,
            self.geo_neighborhood_card,
            self.geo_point_card,
            self.change_use_card,
            self.closed_locales_card,
            self.vut_units_card,
            self.transform_zones_card,
            self.predictive_zones_card,
        ]
        for idx, card in enumerate(intelligence_cards):
            intelligence_grid.addWidget(card, idx // 4, idx % 4)
        self.layout.addLayout(intelligence_grid)

        tables_row_top = QHBoxLayout()
        tables_row_top.setSpacing(14)
        self.events_group = DashboardTablePanel("Eventos Casafari por tipo", ["Tipo", "Cuenta"])
        self.zones_group = DashboardTablePanel(
            "Zonas con baja confianza",
            ["Zona", "Confidence", "Raw", "Geo", "Accion"],
        )
        tables_row_top.addWidget(self.events_group, 1)
        tables_row_top.addWidget(self.zones_group, 1)
        self.layout.addLayout(tables_row_top)

        tables_row_bottom = QHBoxLayout()
        tables_row_bottom.setSpacing(14)
        self.transformation_group = DashboardTablePanel(
            "Zonas con senal transformadora",
            ["Zona", "Transform", "Cambio uso", "Locales cerrados", "VUT", "Accion"],
        )
        self.predictive_group = DashboardTablePanel(
            "Zonas con mejor lectura 30d",
            ["Zona", "Pred 30d", "Banda", "Liquidez", "Heat rel", "Accion"],
        )
        tables_row_bottom.addWidget(self.transformation_group, 1)
        tables_row_bottom.addWidget(self.predictive_group, 1)
        self.layout.addLayout(tables_row_bottom)

        self.sync_group = QGroupBox("Estado del ultimo sync Casafari")
        self.sync_group.setObjectName("DataPanel")
        sync_form = QFormLayout(self.sync_group)
        self.sync_status_label = QLabel("-")
        self.sync_finished_label = QLabel("-")
        self.sync_items_label = QLabel("-")
        self.sync_window_label = QLabel("-")
        self.sync_message_label = QLabel("-")
        self.sync_message_label.setWordWrap(True)

        sync_form.addRow("Estado", self.sync_status_label)
        sync_form.addRow("Ultimo fin", self.sync_finished_label)
        sync_form.addRow("Items vistos", self.sync_items_label)
        sync_form.addRow("Ventana", self.sync_window_label)
        sync_form.addRow("Mensaje", self.sync_message_label)
        self.layout.addWidget(self.sync_group)

        self.refresh()

    def _section_label(self, text: str) -> QLabel:
        label = QLabel(text)
        label.setObjectName("SectionLabel")
        return label

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
        self.assets_card.set_detail("base viva de activos")
        self.buildings_card.set_value(str(stats["buildings"]))
        self.buildings_card.set_detail("nucleo edificio")
        self.contacts_card.set_value(str(stats["contacts"]))
        self.contacts_card.set_detail("contactos consolidados")
        self.listings_card.set_value(str(stats["listings"]))
        self.listings_card.set_detail("baseline + mercado")
        self.events_card.set_value(str(stats["events"]))
        self.events_card.set_detail("eventos historificados")
        self.casafari_raw_card.set_value(str(stats["casafari_raw"]))
        self.casafari_raw_card.set_detail(f"links={safe_text(stats['casafari_links'])}")
        self.casafari_events_card.set_value(str(stats["casafari_events"]))
        self.casafari_events_card.set_detail(
            f"7d={safe_text(stats['casafari_events_7d'])} | 30d={safe_text(stats['casafari_events_30d'])}"
        )
        self.raw_flow_card.set_value(str(stats["casafari_raw_7d"]))
        self.raw_flow_card.set_detail(
            f"30d={safe_text(stats['casafari_raw_30d'])} | sync={safe_text(stats['last_sync_item_count'])}"
        )

        self.casafari_resolved_card.set_value(str(stats["casafari_resolved"]))
        self.casafari_resolved_card.set_detail("match automatico fiable")
        self.casafari_ambiguous_card.set_value(str(stats["casafari_ambiguous"]))
        self.casafari_ambiguous_card.set_detail("requieren revision")
        self.casafari_unresolved_card.set_value(str(stats["casafari_unresolved"]))
        self.casafari_unresolved_card.set_detail("pendientes de linkado")
        self.resolved_ratio_card.set_value(format_pct(stats["casafari_resolved_ratio"]))
        self.resolved_ratio_card.set_detail(f"{resolved_count}/{raw_total} raws")
        self.unresolved_ratio_card.set_value(format_pct(stats["casafari_unresolved_ratio"]))
        self.unresolved_ratio_card.set_detail(f"{unresolved_count}/{raw_total} raws")
        self.raw_price_quality_card.set_value(format_ratio(poor_price_count, raw_total))
        self.raw_price_quality_card.set_detail("sin precio fiable")
        self.raw_address_quality_card.set_value(format_ratio(poor_address_count, raw_total))
        self.raw_address_quality_card.set_detail("direccion debil o zonal")

        self.geo_district_card.set_value(format_ratio(district_count, assets_total))
        self.geo_district_card.set_detail(f"{district_count}/{assets_total} activos")
        self.geo_neighborhood_card.set_value(format_ratio(neighborhood_count, assets_total))
        self.geo_neighborhood_card.set_detail(f"{neighborhood_count}/{assets_total} activos")
        self.geo_point_card.set_value(format_ratio(point_count, assets_total))
        self.geo_point_card.set_detail(f"{point_count}/{assets_total} activos")
        self.change_use_card.set_value(str(stats["total_change_of_use_24m"]))
        self.change_use_card.set_detail(
            f"barrios con cambio={safe_text(stats['neighborhoods_with_change_of_use'])}"
        )
        self.closed_locales_card.set_value(str(stats["total_closed_locales"]))
        self.closed_locales_card.set_detail("stock cerrado contextual")
        self.vut_units_card.set_value(str(stats["total_vut_units"]))
        self.vut_units_card.set_detail("vivienda turistica oficial")
        self.transform_zones_card.set_value(str(stats["transform_zones_count"]))
        self.transform_zones_card.set_detail("transformacion>=65")
        self.predictive_zones_card.set_value(str(stats["predictive_zones_count"]))
        self.predictive_zones_card.set_detail("prediccion 30d>=65")

        self.summary_label.setText(
            f"Resolved {format_pct(stats['casafari_resolved_ratio'])} y unresolved "
            f"{format_pct(stats['casafari_unresolved_ratio'])}. "
            f"Geo con coords {format_ratio(point_count, assets_total)}. "
            f"Transformacion visible en {safe_text(stats['transform_zones_count'])} zonas y "
            f"lectura predictiva fuerte en {safe_text(stats['predictive_zones_count'])}."
        )

        self.events_group.load_rows(
            [
                [
                    safe_text(row.get("event_type")),
                    safe_text(row.get("count")),
                ]
                for row in stats.get("event_type_breakdown", [])
            ]
        )
        self.zones_group.load_rows(
            [
                [
                    safe_text(row.get("zone_label")),
                    safe_text(row.get("zone_confidence_score")),
                    safe_text(row.get("casafari_raw_in_zone")),
                    format_pct(row.get("geo_point_ratio")),
                    safe_text(row.get("recommended_action")),
                ]
                for row in stats.get("low_confidence_zones", [])
            ]
        )
        self.transformation_group.load_rows(
            [
                [
                    safe_text(row.get("zone_label")),
                    safe_text(row.get("zone_transformation_signal_score")),
                    safe_text(row.get("official_change_of_use_24m")),
                    safe_text(row.get("official_locales_closed")),
                    safe_text(row.get("official_vut_units")),
                    safe_text(row.get("recommended_action")),
                ]
                for row in stats.get("top_transformation_zones", [])
            ]
        )
        self.predictive_group.load_rows(
            [
                [
                    safe_text(row.get("zone_label")),
                    safe_text(row.get("predicted_absorption_30d_score")),
                    safe_text(row.get("predicted_absorption_30d_band")),
                    safe_text(row.get("zone_liquidity_score")),
                    safe_text(row.get("zone_relative_heat_score")),
                    safe_text(row.get("recommended_action")),
                ]
                for row in stats.get("top_predictive_zones", [])
            ]
        )

        self.sync_status_label.setText(safe_text(stats["last_sync_status"]))
        self.sync_finished_label.setText(safe_text(stats["last_sync_finished_at"]))
        self.sync_items_label.setText(safe_text(stats["last_sync_item_count"]))
        self.sync_window_label.setText(
            f"{safe_text(stats['last_sync_from'])} -> {safe_text(stats['last_sync_to'])}"
        )
        self.sync_message_label.setText(safe_text(stats["last_sync_message"]))
