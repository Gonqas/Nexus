from PySide6.QtCore import Qt
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
    QTabWidget,
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
    def __init__(self, title: str) -> None:
        super().__init__()
        self.setObjectName("MetricCard")
        self.setMinimumHeight(110)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 16, 18, 16)
        layout.setSpacing(6)

        self.title_label = QLabel(title)
        self.title_label.setObjectName("MetricTitle")
        layout.addWidget(self.title_label)

        self.value_label = QLabel("0")
        self.value_label.setObjectName("MetricValue")
        layout.addWidget(self.value_label)

        self.detail_label = QLabel("")
        self.detail_label.setObjectName("MetricDetail")
        self.detail_label.setWordWrap(True)
        layout.addWidget(self.detail_label)

    def set_content(self, value: str, detail: str) -> None:
        self.value_label.setText(value)
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
        self.table.setMinimumHeight(280)
        layout.addWidget(self.table)

    def load_rows(self, rows: list[list[str]]) -> None:
        self.table.setRowCount(len(rows))
        for row_idx, values in enumerate(rows):
            for col_idx, value in enumerate(values):
                item = QTableWidgetItem(value)
                if col_idx and len(value) <= 10:
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.table.setItem(row_idx, col_idx, item)
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

        layout = QVBoxLayout(container)
        layout.setContentsMargins(10, 10, 10, 24)
        layout.setSpacing(18)

        hero = QWidget()
        hero.setObjectName("HeroPanel")
        hero_layout = QHBoxLayout(hero)
        hero_layout.setContentsMargins(24, 24, 24, 24)
        hero_layout.setSpacing(18)

        copy = QVBoxLayout()
        kicker = QLabel("North star operativo")
        kicker.setObjectName("HeroKicker")
        copy.addWidget(kicker)

        title = QLabel("Dashboard vivo del nucleo")
        title.setObjectName("PageTitle")
        copy.addWidget(title)

        subtitle = QLabel(
            "Menos ruido y mas lectura util. Elige el foco que quieres revisar en vez de verlo todo a la vez."
        )
        subtitle.setObjectName("PageSubtitle")
        subtitle.setWordWrap(True)
        copy.addWidget(subtitle)

        self.summary_label = QLabel("Sin datos")
        self.summary_label.setObjectName("HeroSummary")
        self.summary_label.setWordWrap(True)
        copy.addWidget(self.summary_label)
        copy.addStretch()

        actions = QVBoxLayout()
        actions.addStretch()
        self.refresh_button = QPushButton("Refrescar dashboard")
        self.refresh_button.clicked.connect(self.refresh)
        actions.addWidget(self.refresh_button)

        hero_layout.addLayout(copy, 1)
        hero_layout.addLayout(actions)
        layout.addWidget(hero)

        top_grid = QGridLayout()
        top_grid.setHorizontalSpacing(14)
        top_grid.setVerticalSpacing(14)

        self.assets_card = DashboardMetricCard("Activos")
        self.matching_card = DashboardMetricCard("Matching")
        self.geo_card = DashboardMetricCard("Geo")
        self.transform_card = DashboardMetricCard("Transformacion")
        self.predictive_card = DashboardMetricCard("Prediccion 30d")
        self.sync_card = DashboardMetricCard("Ultimo sync")

        cards = [
            self.assets_card,
            self.matching_card,
            self.geo_card,
            self.transform_card,
            self.predictive_card,
            self.sync_card,
        ]
        for idx, card in enumerate(cards):
            top_grid.addWidget(card, idx // 3, idx % 3)
        layout.addLayout(top_grid)

        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        self._build_matching_tab()
        self._build_geo_tab()
        self._build_territory_tab()
        self._build_sync_tab()

        self.refresh()

    def _build_matching_tab(self) -> None:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setSpacing(14)

        cards_grid = QGridLayout()
        cards_grid.setHorizontalSpacing(14)
        cards_grid.setVerticalSpacing(14)

        self.resolved_card = DashboardMetricCard("Resolved")
        self.unresolved_card = DashboardMetricCard("Unresolved")
        self.ambiguous_card = DashboardMetricCard("Ambiguous")
        self.poor_price_card = DashboardMetricCard("Raw sin precio fiable")
        self.poor_address_card = DashboardMetricCard("Raw direccion pobre")

        for idx, card in enumerate(
            [
                self.resolved_card,
                self.unresolved_card,
                self.ambiguous_card,
                self.poor_price_card,
                self.poor_address_card,
            ]
        ):
            cards_grid.addWidget(card, idx // 3, idx % 3)
        layout.addLayout(cards_grid)

        self.events_group = DashboardTablePanel("Eventos Casafari por tipo", ["Tipo", "Cuenta"])
        self.low_conf_group = DashboardTablePanel(
            "Zonas con baja confianza",
            ["Zona", "Confidence", "Raw", "Geo", "Accion"],
        )
        layout.addWidget(self.events_group)
        layout.addWidget(self.low_conf_group)
        layout.addStretch()

        self.tabs.addTab(page, "Matching")

    def _build_geo_tab(self) -> None:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setSpacing(14)

        cards_grid = QGridLayout()
        cards_grid.setHorizontalSpacing(14)
        cards_grid.setVerticalSpacing(14)

        self.geo_district_card = DashboardMetricCard("Geo distrito")
        self.geo_neighborhood_card = DashboardMetricCard("Geo barrio")
        self.geo_point_card = DashboardMetricCard("Geo coords")
        self.raw_flow_card = DashboardMetricCard("Flow raw 7d")

        for idx, card in enumerate(
            [
                self.geo_district_card,
                self.geo_neighborhood_card,
                self.geo_point_card,
                self.raw_flow_card,
            ]
        ):
            cards_grid.addWidget(card, 0, idx)
        layout.addLayout(cards_grid)
        layout.addStretch()

        self.tabs.addTab(page, "Geo")

    def _build_territory_tab(self) -> None:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setSpacing(14)

        cards_grid = QGridLayout()
        cards_grid.setHorizontalSpacing(14)
        cards_grid.setVerticalSpacing(14)

        self.change_use_card = DashboardMetricCard("Cambios uso 24m")
        self.closed_locales_card = DashboardMetricCard("Locales cerrados")
        self.vut_card = DashboardMetricCard("VUT")
        self.transform_zones_card = DashboardMetricCard("Zonas transformacion")
        self.predictive_zones_card = DashboardMetricCard("Zonas prediccion")

        for idx, card in enumerate(
            [
                self.change_use_card,
                self.closed_locales_card,
                self.vut_card,
                self.transform_zones_card,
                self.predictive_zones_card,
            ]
        ):
            cards_grid.addWidget(card, idx // 3, idx % 3)
        layout.addLayout(cards_grid)

        self.transformation_group = DashboardTablePanel(
            "Zonas con senal transformadora",
            ["Zona", "Transform", "Cambio uso", "Locales cerrados", "VUT", "Accion"],
        )
        self.predictive_group = DashboardTablePanel(
            "Zonas con mejor lectura 30d",
            ["Zona", "Pred 30d", "Banda", "Liquidez", "Heat rel", "Accion"],
        )
        layout.addWidget(self.transformation_group)
        layout.addWidget(self.predictive_group)
        layout.addStretch()

        self.tabs.addTab(page, "Territorio")

    def _build_sync_tab(self) -> None:
        page = QWidget()
        layout = QVBoxLayout(page)

        self.sync_group = QGroupBox("Estado del ultimo sync Casafari")
        self.sync_group.setObjectName("DataPanel")
        form = QFormLayout(self.sync_group)

        self.sync_status_label = QLabel("-")
        self.sync_finished_label = QLabel("-")
        self.sync_items_label = QLabel("-")
        self.sync_window_label = QLabel("-")
        self.sync_message_label = QLabel("-")
        self.sync_message_label.setWordWrap(True)

        form.addRow("Estado", self.sync_status_label)
        form.addRow("Ultimo fin", self.sync_finished_label)
        form.addRow("Items vistos", self.sync_items_label)
        form.addRow("Ventana", self.sync_window_label)
        form.addRow("Mensaje", self.sync_message_label)
        layout.addWidget(self.sync_group)
        layout.addStretch()

        self.tabs.addTab(page, "Sync")

    def refresh(self) -> None:
        with SessionLocal() as session:
            stats = get_dashboard_stats(session)

        assets_total = int(stats["assets"])
        raw_total = int(stats["casafari_raw"])
        point_count = int(stats["assets_with_geo_point"])
        district_count = int(stats["assets_with_district"])
        neighborhood_count = int(stats["assets_with_neighborhood"])

        self.summary_label.setText(
            f"Resolved {format_pct(stats['casafari_resolved_ratio'])}, geo con coords {format_ratio(point_count, assets_total)} "
            f"y transformacion visible en {safe_text(stats['transform_zones_count'])} zonas."
        )

        self.assets_card.set_content(
            str(stats["assets"]),
            f"{stats['buildings']} edificios | {stats['listings']} listings",
        )
        self.matching_card.set_content(
            format_pct(stats["casafari_resolved_ratio"]),
            f"{stats['casafari_resolved']}/{raw_total} raws resueltos",
        )
        self.geo_card.set_content(
            format_ratio(point_count, assets_total),
            f"{point_count}/{assets_total} activos con coords",
        )
        self.transform_card.set_content(
            str(stats["transform_zones_count"]),
            f"cambios uso 24m={safe_text(stats['total_change_of_use_24m'])}",
        )
        self.predictive_card.set_content(
            str(stats["predictive_zones_count"]),
            "zonas con lectura 30d fuerte",
        )
        self.sync_card.set_content(
            safe_text(stats["last_sync_status"]),
            f"items {safe_text(stats['last_sync_item_count'])}",
        )

        self.resolved_card.set_content(
            format_pct(stats["casafari_resolved_ratio"]),
            f"{stats['casafari_resolved']} raws resueltos",
        )
        self.unresolved_card.set_content(
            format_pct(stats["casafari_unresolved_ratio"]),
            f"{stats['casafari_unresolved']} sin resolver",
        )
        self.ambiguous_card.set_content(
            safe_text(stats["casafari_ambiguous"]),
            "casos que piden decision",
        )
        self.poor_price_card.set_content(
            format_ratio(int(stats["raws_without_reliable_price"]), raw_total),
            "precio debil o dudoso",
        )
        self.poor_address_card.set_content(
            format_ratio(int(stats["raws_with_poor_address"]), raw_total),
            "direccion zonal o pobre",
        )

        self.geo_district_card.set_content(
            format_ratio(district_count, assets_total),
            f"{district_count}/{assets_total} activos",
        )
        self.geo_neighborhood_card.set_content(
            format_ratio(neighborhood_count, assets_total),
            f"{neighborhood_count}/{assets_total} activos",
        )
        self.geo_point_card.set_content(
            format_ratio(point_count, assets_total),
            f"{point_count}/{assets_total} activos",
        )
        self.raw_flow_card.set_content(
            safe_text(stats["casafari_raw_7d"]),
            f"30d={safe_text(stats['casafari_raw_30d'])}",
        )

        self.change_use_card.set_content(
            safe_text(stats["total_change_of_use_24m"]),
            f"barrios activos={safe_text(stats['neighborhoods_with_change_of_use'])}",
        )
        self.closed_locales_card.set_content(
            safe_text(stats["total_closed_locales"]),
            "stock cerrado oficial",
        )
        self.vut_card.set_content(
            safe_text(stats["total_vut_units"]),
            "vivienda turistica oficial",
        )
        self.transform_zones_card.set_content(
            safe_text(stats["transform_zones_count"]),
            "score transformacion >= 65",
        )
        self.predictive_zones_card.set_content(
            safe_text(stats["predictive_zones_count"]),
            "prediccion 30d >= 65",
        )

        self.events_group.load_rows(
            [
                [safe_text(row.get("event_type")), safe_text(row.get("count"))]
                for row in stats.get("event_type_breakdown", [])
            ]
        )
        self.low_conf_group.load_rows(
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
