from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QComboBox,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QScrollArea,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from app.ui.widgets.leaflet_map_widget import LeafletMapWidget
from core.services.spatial_map_service import get_spatial_map_payload
from db.session import SessionLocal


def safe_text(value) -> str:
    if value is None or value == "":
        return "-"
    return str(value)


def safe_float(value, decimals: int = 1) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):.{decimals}f}"
    except (TypeError, ValueError):
        return str(value)


class SpatialStatCard(QGroupBox):
    def __init__(self, title: str) -> None:
        super().__init__(title)
        self.setMinimumHeight(102)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 14, 16, 14)
        layout.setSpacing(4)

        self.value_label = QLabel("0")
        self.value_label.setStyleSheet("font-size: 27px; font-weight: bold;")
        layout.addWidget(self.value_label)

        self.detail_label = QLabel("")
        self.detail_label.setObjectName("PageSubtitle")
        self.detail_label.setWordWrap(True)
        layout.addWidget(self.detail_label)
        layout.addStretch()

    def set_content(self, value: str, detail: str) -> None:
        self.value_label.setText(value)
        self.detail_label.setText(detail)


class SpatialTable(QGroupBox):
    def __init__(self, title: str, headers: list[str]) -> None:
        super().__init__(title)

        layout = QVBoxLayout(self)
        self.table = QTableWidget()
        self.table.setColumnCount(len(headers))
        self.table.setHorizontalHeaderLabels(headers)
        self.table.verticalHeader().setVisible(False)
        self.table.setAlternatingRowColors(True)
        self.table.setCornerButtonEnabled(False)
        self.table.setMinimumHeight(220)
        layout.addWidget(self.table)


class MapView(QWidget):
    def __init__(self) -> None:
        super().__init__()

        self.payload: dict | None = None
        self.selected_event_id: int | None = None
        self.selected_microzone_label: str | None = None
        self.selected_zone_label: str | None = None

        root_layout = QVBoxLayout(self)
        root_layout.setContentsMargins(0, 0, 0, 0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        root_layout.addWidget(scroll)

        page = QWidget()
        scroll.setWidget(page)

        layout = QVBoxLayout(page)
        layout.setContentsMargins(10, 8, 10, 20)
        layout.setSpacing(16)

        header = QHBoxLayout()
        title_box = QVBoxLayout()

        title = QLabel("Mapa operativo")
        title.setStyleSheet("font-size: 22px; font-weight: bold;")
        title_box.addWidget(title)

        subtitle = QLabel(
            "Vista espacial para leer oportunidades geolocalizadas y microzonas sin perder el contexto operativo."
        )
        subtitle.setObjectName("PageSubtitle")
        subtitle.setWordWrap(True)
        title_box.addWidget(subtitle)

        controls = QHBoxLayout()
        self.window_combo = QComboBox()
        self.window_combo.addItems(["7", "14", "30"])
        self.window_combo.setCurrentText("14")
        self.window_combo.currentTextChanged.connect(self.load_data)

        self.event_combo = QComboBox()
        self.event_combo.addItems(
            [
                "all",
                "listing_detected",
                "price_drop",
                "price_raise",
                "reserved",
                "sold",
                "not_available",
                "expired",
            ]
        )
        self.event_combo.currentTextChanged.connect(self.load_data)

        self.score_combo = QComboBox()
        self.score_combo.addItems(["all", "40", "50", "60"])
        self.score_combo.currentTextChanged.connect(self.load_data)

        self.layer_combo = QComboBox()
        self.layer_combo.addItems(["both", "opportunities", "microzones"])
        self.layer_combo.currentTextChanged.connect(self.load_data)

        self.boundary_combo = QComboBox()
        self.boundary_combo.addItems(["none", "districts", "neighborhoods"])
        self.boundary_combo.setCurrentText("neighborhoods")
        self.boundary_combo.currentTextChanged.connect(self.load_data)

        self.metric_combo = QComboBox()
        self.metric_combo.addItems(
            [
                "capture",
                "heat",
                "relative_heat",
                "pressure",
                "transformation",
                "predictive",
                "confidence",
                "liquidity",
            ]
        )
        self.metric_combo.currentTextChanged.connect(self.load_data)

        self.heat_combo = QComboBox()
        self.heat_combo.addItems(["on", "off"])
        self.heat_combo.currentTextChanged.connect(self.load_data)

        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Filtrar por zona, microzona o direccion")
        self.search_input.returnPressed.connect(self.load_data)

        self.refresh_button = QPushButton("Refrescar mapa")
        self.refresh_button.clicked.connect(self.load_data)

        controls.addWidget(QLabel("Ventana:"))
        controls.addWidget(self.window_combo)
        controls.addWidget(QLabel("Evento:"))
        controls.addWidget(self.event_combo)
        controls.addWidget(QLabel("Score min:"))
        controls.addWidget(self.score_combo)
        controls.addWidget(QLabel("Capas:"))
        controls.addWidget(self.layer_combo)
        controls.addWidget(QLabel("Superficie:"))
        controls.addWidget(self.boundary_combo)
        controls.addWidget(QLabel("Metrica:"))
        controls.addWidget(self.metric_combo)
        controls.addWidget(QLabel("Heat:"))
        controls.addWidget(self.heat_combo)
        controls.addWidget(self.search_input)
        controls.addWidget(self.refresh_button)

        header.addLayout(title_box, 1)
        header.addLayout(controls)
        layout.addLayout(header)

        self.summary_label = QLabel("Sin lectura espacial")
        self.summary_label.setObjectName("PageSubtitle")
        self.summary_label.setWordWrap(True)
        layout.addWidget(self.summary_label)

        summary_grid = QGridLayout()
        summary_grid.setHorizontalSpacing(12)
        summary_grid.setVerticalSpacing(12)

        self.geo_card = SpatialStatCard("Geo oportunidades")
        self.high_card = SpatialStatCard("Alta prioridad")
        self.score_card = SpatialStatCard("Score medio")
        self.micro_card = SpatialStatCard("Microzonas")
        self.zone_card = SpatialStatCard("Zonas cartografiadas")

        summary_grid.addWidget(self.geo_card, 0, 0)
        summary_grid.addWidget(self.high_card, 0, 1)
        summary_grid.addWidget(self.score_card, 0, 2)
        summary_grid.addWidget(self.micro_card, 0, 3)
        summary_grid.addWidget(self.zone_card, 0, 4)
        layout.addLayout(summary_grid)

        splitter = QSplitter()
        layout.addWidget(splitter, 1)

        self.map_group = QGroupBox("Mapa")
        map_layout = QVBoxLayout(self.map_group)
        map_layout.setContentsMargins(10, 10, 10, 10)
        self.map_widget = LeafletMapWidget()
        map_layout.addWidget(self.map_widget)
        splitter.addWidget(self.map_group)

        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(12)

        self.focus_group = QGroupBox("Foco espacial")
        focus_layout = QVBoxLayout(self.focus_group)
        self.focus_label = QLabel(
            "Selecciona una oportunidad o una microzona para llevar el foco al mapa y leerla mejor."
        )
        self.focus_label.setWordWrap(True)
        focus_layout.addWidget(self.focus_label)
        right_layout.addWidget(self.focus_group)

        self.opportunities_table = SpatialTable(
            "Top oportunidades geolocalizadas",
            ["Score", "Evento", "Zona", "Portal", "Direccion"],
        )
        self.opportunities_table.table.itemSelectionChanged.connect(
            self.on_opportunity_selected
        )
        right_layout.addWidget(self.opportunities_table)

        self.microzones_table = SpatialTable(
            "Top microzonas",
            ["Capture", "Conf", "Base", "Eventos", "Microzona"],
        )
        self.microzones_table.table.itemSelectionChanged.connect(self.on_microzone_selected)
        right_layout.addWidget(self.microzones_table)

        self.zones_table = SpatialTable(
            "Top zonas en superficie",
            ["Metrica", "Zona", "Conf", "Accion", "Resumen"],
        )
        self.zones_table.table.itemSelectionChanged.connect(self.on_zone_selected)
        right_layout.addWidget(self.zones_table)

        splitter.addWidget(right_panel)
        splitter.setSizes([1120, 560])

        self.load_data()

    def _window_days(self) -> int:
        return int(self.window_combo.currentText())

    def _min_score(self) -> float | None:
        text = self.score_combo.currentText()
        if text == "all":
            return None
        return float(text)

    def load_data(self) -> None:
        self.selected_event_id = None
        self.selected_microzone_label = None
        self.selected_zone_label = None

        with SessionLocal() as session:
            self.payload = get_spatial_map_payload(
                session,
                window_days=self._window_days(),
                event_type_filter=self.event_combo.currentText(),
                min_score=self._min_score(),
                zone_query=self.search_input.text(),
                layer_mode=self.layer_combo.currentText(),
                boundary_level=self.boundary_combo.currentText(),
                zone_metric_mode=self.metric_combo.currentText(),
                heat_mode=self.heat_combo.currentText(),
            )

        self.render_payload()

    def focus_context(
        self,
        *,
        zone_label: str | None = None,
        microzone_label: str | None = None,
        event_id: int | None = None,
        window_days: int | None = None,
    ) -> None:
        if window_days is not None:
            self.window_combo.setCurrentText(str(window_days))

        if zone_label:
            self.search_input.setText(zone_label)
            self.boundary_combo.setCurrentText("neighborhoods")
        elif microzone_label:
            self.search_input.setText(microzone_label)
        elif event_id is not None:
            self.layer_combo.setCurrentText("opportunities")

        with SessionLocal() as session:
            self.payload = get_spatial_map_payload(
                session,
                window_days=self._window_days(),
                event_type_filter=self.event_combo.currentText(),
                min_score=self._min_score(),
                zone_query=self.search_input.text(),
                layer_mode=self.layer_combo.currentText(),
                boundary_level=self.boundary_combo.currentText(),
                zone_metric_mode=self.metric_combo.currentText(),
                heat_mode=self.heat_combo.currentText(),
            )

        self.selected_event_id = event_id
        self.selected_microzone_label = microzone_label
        self.selected_zone_label = zone_label
        self.render_payload()
        self._select_external_focus()

    def render_payload(self) -> None:
        payload = self.payload or {}
        summary = payload.get("summary") or {}

        self.summary_label.setText(
            f"{summary.get('geo_opportunities_total', 0)} oportunidades geolocalizadas y "
            f"{summary.get('microzones_total', 0)} microzonas en ventana {payload.get('window_days', 14)}d. "
            f"Capas activas: {safe_text(payload.get('layer_mode'))}. "
            f"Superficie: {safe_text(payload.get('boundary_level'))} | "
            f"metrica: {safe_text((payload.get('zone_metric') or {}).get('label'))}."
        )

        self.geo_card.set_content(
            safe_text(summary.get("geo_opportunities_total", 0)),
            "oportunidades con coordenadas",
        )
        self.high_card.set_content(
            safe_text(summary.get("high_priority_geo_opportunities", 0)),
            "prioridad alta en mapa",
        )
        self.score_card.set_content(
            safe_float(summary.get("avg_opportunity_score", 0.0)),
            "score medio de las geo",
        )
        self.micro_card.set_content(
            safe_text(summary.get("microzones_total", 0)),
            f"hotspots micro {safe_text(summary.get('microzone_hotspots', 0))}",
        )
        self.zone_card.set_content(
            safe_text(summary.get("zones_with_boundaries", 0)),
            f"surface {safe_text(payload.get('boundary_level'))}",
        )

        self._render_opportunities(payload.get("top_opportunities") or [])
        self._render_microzones(payload.get("top_microzones") or [])
        self._render_zones(payload.get("top_zones") or [])
        self._refresh_map()

    def _select_external_focus(self) -> None:
        if self.selected_event_id is not None:
            rows = (self.payload or {}).get("top_opportunities") or []
            for idx, row in enumerate(rows):
                if row.get("event_id") == self.selected_event_id:
                    self.opportunities_table.table.selectRow(idx)
                    self.on_opportunity_selected()
                    return

        if self.selected_microzone_label:
            rows = (self.payload or {}).get("top_microzones") or []
            for idx, row in enumerate(rows):
                if row.get("microzone_label") == self.selected_microzone_label:
                    self.microzones_table.table.selectRow(idx)
                    self.on_microzone_selected()
                    return

        if self.selected_zone_label:
            rows = (self.payload or {}).get("top_zones") or []
            target = safe_text(self.selected_zone_label)
            for idx, row in enumerate(rows):
                if safe_text(row.get("zone_label")) == target:
                    self.zones_table.table.selectRow(idx)
                    self.on_zone_selected()
                    return

    def _render_opportunities(self, rows: list[dict]) -> None:
        table = self.opportunities_table.table
        table.setRowCount(len(rows))

        for row_idx, row in enumerate(rows):
            values = [
                safe_float(row.get("score")),
                safe_text(row.get("event_type")),
                safe_text(row.get("zone_label")),
                safe_text(row.get("portal")),
                safe_text(row.get("asset_address")),
            ]
            for col_idx, value in enumerate(values):
                item = QTableWidgetItem(value)
                if col_idx == 0:
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                table.setItem(row_idx, col_idx, item)

        table.resizeColumnsToContents()
        if rows:
            table.selectRow(0)

    def _render_microzones(self, rows: list[dict]) -> None:
        table = self.microzones_table.table
        table.setRowCount(len(rows))

        for row_idx, row in enumerate(rows):
            values = [
                safe_float(row.get("microzone_capture_score")),
                safe_float(row.get("microzone_confidence_score")),
                safe_text(row.get("parent_zone_label")),
                safe_text(row.get("events_14d")),
                safe_text(row.get("microzone_label")),
            ]
            for col_idx, value in enumerate(values):
                item = QTableWidgetItem(value)
                if col_idx in (0, 1, 3):
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                table.setItem(row_idx, col_idx, item)

        table.resizeColumnsToContents()

    def _render_zones(self, rows: list[dict]) -> None:
        table = self.zones_table.table
        table.setRowCount(len(rows))
        metric_key = ((self.payload or {}).get("zone_metric") or {}).get("key") or "zone_capture_score"

        for row_idx, row in enumerate(rows):
            values = [
                safe_float(row.get(metric_key)),
                safe_text(row.get("zone_label")),
                safe_float(row.get("zone_confidence_score")),
                safe_text(row.get("recommended_action")),
                safe_text(row.get("executive_summary") or row.get("score_explanation")),
            ]
            for col_idx, value in enumerate(values):
                item = QTableWidgetItem(value)
                if col_idx in (0, 2):
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                table.setItem(row_idx, col_idx, item)

        table.resizeColumnsToContents()

    def _refresh_map(self) -> None:
        if not self.payload:
            return

        self.map_widget.load_payload(
            self.payload,
            selection={
                "selected_event_id": self.selected_event_id,
                "selected_microzone_label": self.selected_microzone_label,
                "selected_zone_label": self.selected_zone_label,
            },
        )

    def on_opportunity_selected(self) -> None:
        rows = (self.payload or {}).get("top_opportunities") or []
        items = self.opportunities_table.table.selectedItems()
        if not items:
            return

        row_idx = items[0].row()
        if row_idx < 0 or row_idx >= len(rows):
            return

        row = rows[row_idx]
        self.selected_event_id = row.get("event_id")
        self.selected_microzone_label = None
        self.selected_zone_label = row.get("zone_label")
        self.focus_label.setText(
            f"Oportunidad #{safe_text(row.get('event_id'))}\n"
            f"{safe_text(row.get('asset_address'))}\n"
            f"Score {safe_float(row.get('score'))} | {safe_text(row.get('priority_label'))}\n"
            f"{safe_text(row.get('zone_label'))} | {safe_text(row.get('reason'))}"
        )
        self._refresh_map()

    def on_microzone_selected(self) -> None:
        rows = (self.payload or {}).get("top_microzones") or []
        items = self.microzones_table.table.selectedItems()
        if not items:
            return

        row_idx = items[0].row()
        if row_idx < 0 or row_idx >= len(rows):
            return

        row = rows[row_idx]
        self.selected_microzone_label = row.get("microzone_label")
        self.selected_event_id = None
        self.selected_zone_label = row.get("parent_zone_label")
        self.focus_label.setText(
            f"{safe_text(row.get('microzone_label'))}\n"
            f"Base: {safe_text(row.get('parent_zone_label'))}\n"
            f"Capture {safe_float(row.get('microzone_capture_score'))} | "
            f"Confianza {safe_float(row.get('microzone_confidence_score'))}\n"
            f"{safe_text(row.get('recommended_action'))} | {safe_text(row.get('radar_explanation'))}"
        )
        self._refresh_map()

    def on_zone_selected(self) -> None:
        rows = (self.payload or {}).get("top_zones") or []
        items = self.zones_table.table.selectedItems()
        if not items:
            return

        row_idx = items[0].row()
        if row_idx < 0 or row_idx >= len(rows):
            return

        row = rows[row_idx]
        metric = (self.payload or {}).get("zone_metric") or {}
        self.selected_zone_label = row.get("zone_label")
        self.selected_event_id = None
        self.selected_microzone_label = None
        self.focus_label.setText(
            f"{safe_text(row.get('zone_label'))}\n"
            f"{safe_text(metric.get('label'))}: {safe_float(row.get(metric.get('key')))} | "
            f"Confianza {safe_float(row.get('zone_confidence_score'))}\n"
            f"{safe_text(row.get('recommended_action'))}\n"
            f"{safe_text(row.get('executive_summary') or row.get('score_explanation'))}"
        )
        self._refresh_map()
