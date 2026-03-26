from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QFrame,
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
    QComboBox,
)

from db.repositories.dashboard_repo import get_dashboard_stats
from db.session import SessionLocal


def safe_text(value) -> str:
    if value is None or value == "":
        return "-"
    return str(value)


def format_ratio(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "0%"
    return f"{(numerator / denominator) * 100:.0f}%"


def format_pct(value: float | None) -> str:
    if value is None:
        return "0%"
    return f"{float(value) * 100:.0f}%"


class DashboardMetricCard(QWidget):
    def __init__(self, title: str) -> None:
        super().__init__()
        self.setObjectName("MetricCard")
        self.setMinimumHeight(104)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 16, 18, 16)
        layout.setSpacing(5)

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
        self.table.setMinimumHeight(240)
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
        layout.setSpacing(16)

        hero = QWidget()
        hero.setObjectName("HeroPanel")
        hero_layout = QVBoxLayout(hero)
        hero_layout.setContentsMargins(22, 22, 22, 22)
        hero_layout.setSpacing(12)

        top_row = QHBoxLayout()
        copy = QVBoxLayout()
        kicker = QLabel("Resumen operativo")
        kicker.setObjectName("HeroKicker")
        copy.addWidget(kicker)

        title = QLabel("Qué mirar primero")
        title.setObjectName("PageTitle")
        copy.addWidget(title)

        subtitle = QLabel(
            "Esta pantalla enseña solo lo esencial. Si quieres más detalle, cambia de bloque o usa filtros."
        )
        subtitle.setObjectName("PageSubtitle")
        subtitle.setWordWrap(True)
        copy.addWidget(subtitle)
        top_row.addLayout(copy, 1)

        controls = QHBoxLayout()
        controls.addWidget(QLabel("Top visibles:"))
        self.limit_combo = QComboBox()
        self.limit_combo.addItems(["5", "10", "20"])
        self.limit_combo.setCurrentText("5")
        self.limit_combo.currentTextChanged.connect(self.refresh)
        controls.addWidget(self.limit_combo)

        self.refresh_button = QPushButton("Actualizar")
        self.refresh_button.setObjectName("GhostButton")
        self.refresh_button.clicked.connect(self.refresh)
        controls.addWidget(self.refresh_button)
        top_row.addLayout(controls)

        hero_layout.addLayout(top_row)

        self.summary_label = QLabel("Sin datos")
        self.summary_label.setObjectName("HeroSummary")
        self.summary_label.setWordWrap(True)
        hero_layout.addWidget(self.summary_label)
        layout.addWidget(hero)

        top_grid = QGridLayout()
        top_grid.setHorizontalSpacing(14)
        top_grid.setVerticalSpacing(14)

        self.base_card = DashboardMetricCard("Base")
        self.casafari_card = DashboardMetricCard("Casafari")
        self.geo_card = DashboardMetricCard("Cobertura geo")
        self.next_action_card = DashboardMetricCard("Siguiente foco")

        for idx, card in enumerate(
            [self.base_card, self.casafari_card, self.geo_card, self.next_action_card]
        ):
            top_grid.addWidget(card, 0, idx)
        layout.addLayout(top_grid)

        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        self._build_overview_tab()
        self._build_market_tab()
        self._build_territory_tab()
        self._build_system_tab()

        self.refresh()

    def _build_overview_tab(self) -> None:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setSpacing(14)

        cards = QGridLayout()
        cards.setHorizontalSpacing(14)
        cards.setVerticalSpacing(14)

        self.review_card = DashboardMetricCard("Casos claros")
        self.pending_card = DashboardMetricCard("Casos por revisar")
        self.address_card = DashboardMetricCard("Direcciones flojas")
        self.zone_card = DashboardMetricCard("Zonas poco fiables")

        for idx, card in enumerate(
            [self.review_card, self.pending_card, self.address_card, self.zone_card]
        ):
            cards.addWidget(card, idx // 2, idx % 2)
        layout.addLayout(cards)

        self.low_conf_group = DashboardTablePanel(
            "Zonas que hoy necesitan limpieza o más dato",
            ["Zona", "Confianza", "Raw", "Geo", "Qué hacer"],
        )
        layout.addWidget(self.low_conf_group)
        layout.addStretch()

        self.tabs.addTab(page, "Resumen")

    def _build_market_tab(self) -> None:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setSpacing(14)

        cards = QGridLayout()
        cards.setHorizontalSpacing(14)
        cards.setVerticalSpacing(14)

        self.resolved_card = DashboardMetricCard("Matching claro")
        self.unresolved_card = DashboardMetricCard("Sin resolver")
        self.raw_flow_card = DashboardMetricCard("Raws recientes")
        self.price_card = DashboardMetricCard("Precios dudosos")

        for idx, card in enumerate(
            [self.resolved_card, self.unresolved_card, self.raw_flow_card, self.price_card]
        ):
            cards.addWidget(card, idx // 2, idx % 2)
        layout.addLayout(cards)

        self.events_group = DashboardTablePanel(
            "Tipos de evento más frecuentes",
            ["Evento", "Cuenta"],
        )
        layout.addWidget(self.events_group)
        layout.addStretch()

        self.tabs.addTab(page, "Casafari")

    def _build_territory_tab(self) -> None:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setSpacing(14)

        cards = QGridLayout()
        cards.setHorizontalSpacing(14)
        cards.setVerticalSpacing(14)

        self.transform_card = DashboardMetricCard("Transformación")
        self.predictive_card = DashboardMetricCard("Lectura 30d")
        self.closed_card = DashboardMetricCard("Locales cerrados")
        self.vut_card = DashboardMetricCard("VUT")

        for idx, card in enumerate(
            [self.transform_card, self.predictive_card, self.closed_card, self.vut_card]
        ):
            cards.addWidget(card, idx // 2, idx % 2)
        layout.addLayout(cards)

        self.transformation_group = DashboardTablePanel(
            "Barrios con señal transformadora",
            ["Zona", "Señal", "Cambio uso", "Locales", "Acción"],
        )
        self.predictive_group = DashboardTablePanel(
            "Barrios con mejor lectura a 30 días",
            ["Zona", "Pred 30d", "Banda", "Liquidez", "Acción"],
        )
        layout.addWidget(self.transformation_group)
        layout.addWidget(self.predictive_group)
        layout.addStretch()

        self.tabs.addTab(page, "Territorio")

    def _build_system_tab(self) -> None:
        page = QWidget()
        layout = QVBoxLayout(page)

        self.sync_group = QGroupBox("Estado del último sync")
        self.sync_group.setObjectName("DataPanel")
        sync_layout = QVBoxLayout(self.sync_group)

        self.sync_status_label = QLabel("-")
        self.sync_status_label.setObjectName("MetricValue")
        sync_layout.addWidget(self.sync_status_label)

        self.sync_meta_label = QLabel("-")
        self.sync_meta_label.setObjectName("MetricDetail")
        self.sync_meta_label.setWordWrap(True)
        sync_layout.addWidget(self.sync_meta_label)

        self.sync_message_label = QLabel("-")
        self.sync_message_label.setWordWrap(True)
        sync_layout.addWidget(self.sync_message_label)

        layout.addWidget(self.sync_group)
        layout.addStretch()

        self.tabs.addTab(page, "Sistema")

    def _limit(self) -> int:
        return int(self.limit_combo.currentText())

    def refresh(self) -> None:
        with SessionLocal() as session:
            stats = get_dashboard_stats(session)

        top_n = self._limit()
        assets_total = int(stats.get("assets") or 0)
        raw_total = int(stats.get("casafari_raw") or 0)
        point_count = int(stats.get("assets_with_geo_point") or 0)
        low_conf_rows = stats.get("low_confidence_zones", [])[:top_n]
        top_transform = stats.get("top_transformation_zones", [])[:top_n]
        top_predictive = stats.get("top_predictive_zones", [])[:top_n]

        self.summary_label.setText(
            "Empieza por 'Casos por revisar' si Casafari va flojo; "
            "si no, revisa 'Zonas poco fiables' para ver dónde falta limpiar o enriquecer dato."
        )

        unresolved = int(stats.get("casafari_unresolved") or 0)
        poor_address = int(stats.get("raws_with_poor_address") or 0)
        next_focus = "matching" if unresolved >= poor_address else "geografía"
        next_focus_detail = (
            f"{unresolved} sin resolver" if next_focus == "matching" else f"{poor_address} direcciones flojas"
        )

        self.base_card.set_content(
            str(assets_total),
            f"{stats.get('buildings', 0)} edificios y {stats.get('listings', 0)} listings",
        )
        self.casafari_card.set_content(
            format_pct(stats.get("casafari_resolved_ratio")),
            f"{stats.get('casafari_resolved', 0)} de {raw_total} raws ya tienen destino claro",
        )
        self.geo_card.set_content(
            format_ratio(point_count, assets_total),
            f"{point_count} activos tienen coordenadas útiles",
        )
        self.next_action_card.set_content(next_focus, next_focus_detail)

        self.review_card.set_content(
            format_pct(stats.get("casafari_resolved_ratio")),
            f"{stats.get('casafari_resolved', 0)} casos claros",
        )
        self.pending_card.set_content(
            str(unresolved),
            "casos que todavía piden revisión",
        )
        self.address_card.set_content(
            str(poor_address),
            "raws con dirección débil o poco útil",
        )
        self.zone_card.set_content(
            str(len(low_conf_rows)),
            "zonas en el top visible con confianza baja",
        )

        self.resolved_card.set_content(
            format_pct(stats.get("casafari_resolved_ratio")),
            f"{stats.get('casafari_resolved', 0)} resueltos",
        )
        self.unresolved_card.set_content(
            format_pct(stats.get("casafari_unresolved_ratio")),
            f"{unresolved} pendientes",
        )
        self.raw_flow_card.set_content(
            safe_text(stats.get("casafari_raw_7d")),
            f"30d: {safe_text(stats.get('casafari_raw_30d'))}",
        )
        self.price_card.set_content(
            str(int(stats.get("raws_without_reliable_price") or 0)),
            "raws con precio poco fiable",
        )

        self.transform_card.set_content(
            str(int(stats.get("transform_zones_count") or 0)),
            "barrios con señal transformadora fuerte",
        )
        self.predictive_card.set_content(
            str(int(stats.get("predictive_zones_count") or 0)),
            "barrios con lectura 30d fuerte",
        )
        self.closed_card.set_content(
            safe_text(stats.get("total_closed_locales")),
            "stock oficial de locales cerrados",
        )
        self.vut_card.set_content(
            safe_text(stats.get("total_vut_units")),
            "viviendas de uso turístico oficiales",
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
                for row in low_conf_rows
            ]
        )
        self.events_group.load_rows(
            [
                [safe_text(row.get("event_type")), safe_text(row.get("count"))]
                for row in (stats.get("event_type_breakdown", [])[:top_n])
            ]
        )
        self.transformation_group.load_rows(
            [
                [
                    safe_text(row.get("zone_label")),
                    safe_text(row.get("zone_transformation_signal_score")),
                    safe_text(row.get("official_change_of_use_24m")),
                    safe_text(row.get("official_locales_closed")),
                    safe_text(row.get("recommended_action")),
                ]
                for row in top_transform
            ]
        )
        self.predictive_group.load_rows(
            [
                [
                    safe_text(row.get("zone_label")),
                    safe_text(row.get("predicted_absorption_30d_score")),
                    safe_text(row.get("predicted_absorption_30d_band")),
                    safe_text(row.get("zone_liquidity_score")),
                    safe_text(row.get("recommended_action")),
                ]
                for row in top_predictive
            ]
        )

        self.sync_status_label.setText(safe_text(stats.get("last_sync_status")))
        self.sync_meta_label.setText(
            f"Items: {safe_text(stats.get('last_sync_item_count'))} | "
            f"Ventana: {safe_text(stats.get('last_sync_from'))} -> {safe_text(stats.get('last_sync_to'))}"
        )
        self.sync_message_label.setText(safe_text(stats.get("last_sync_message")))
