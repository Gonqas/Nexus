from __future__ import annotations

from PySide6.QtCore import Qt, Signal
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
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from core.services.radar_service_v2 import get_radar_payload_v2
from db.session import SessionLocal


def safe_text(value) -> str:
    if value is None or value == "":
        return "-"
    return str(value)


class StatCard(QGroupBox):
    def __init__(self, title: str) -> None:
        super().__init__(title)
        self.setMinimumHeight(102)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 14, 16, 14)
        layout.setSpacing(4)

        self.value_label = QLabel("0")
        self.value_label.setObjectName("MetricValue")
        layout.addWidget(self.value_label)

        self.detail_label = QLabel("")
        self.detail_label.setObjectName("MetricDetail")
        self.detail_label.setWordWrap(True)
        layout.addWidget(self.detail_label)
        layout.addStretch()

    def set_content(self, value: str, detail: str) -> None:
        self.value_label.setText(value)
        self.detail_label.setText(detail)


class RadarTable(QGroupBox):
    row_selected = Signal(dict)

    def __init__(self, title: str, metric_label: str) -> None:
        super().__init__(title)
        self.rows: list[dict] = []
        self.metric_label = metric_label

        layout = QVBoxLayout(self)
        self.table = QTableWidget()
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(
            ["Zona", metric_label, "Confianza", "Acción", "Explicación"]
        )
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        self.table.setCornerButtonEnabled(False)
        self.table.setMinimumHeight(320)
        self.table.itemSelectionChanged.connect(self._emit_selection)
        layout.addWidget(self.table)

    def load_rows(self, rows: list[dict], metric_key: str) -> None:
        self.rows = list(rows)
        self.table.setRowCount(len(rows))

        for row_idx, row in enumerate(rows):
            confidence_value = row.get("zone_confidence_score")
            if confidence_value is None:
                confidence_value = row.get("microzone_confidence_score")

            values = [
                safe_text(row.get("zone_label")),
                safe_text(row.get(metric_key)),
                safe_text(confidence_value),
                safe_text(row.get("recommended_action")),
                safe_text(row.get("radar_explanation")),
            ]
            for col_idx, value in enumerate(values):
                item = QTableWidgetItem(value)
                if col_idx in (1, 2):
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.table.setItem(row_idx, col_idx, item)

        self.table.resizeColumnsToContents()

    def _emit_selection(self) -> None:
        items = self.table.selectedItems()
        if not items:
            return
        row_idx = items[0].row()
        if row_idx < 0 or row_idx >= len(self.rows):
            return
        self.row_selected.emit(self.rows[row_idx])


class RadarView(QWidget):
    open_in_map_requested = Signal(dict)

    def __init__(self) -> None:
        super().__init__()
        self.selected_row_payload: dict | None = None

        root_layout = QVBoxLayout(self)
        root_layout.setContentsMargins(0, 0, 0, 0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        root_layout.addWidget(scroll)

        page = QWidget()
        page.setObjectName("PageScrollContainer")
        scroll.setWidget(page)

        layout = QVBoxLayout(page)
        layout.setContentsMargins(10, 8, 10, 20)
        layout.setSpacing(16)

        title = QLabel("Zonas")
        title.setObjectName("PageTitle")
        layout.addWidget(title)

        subtitle = QLabel(
            "El radar ya no enseña todo a la vez. Elige un foco y compara solo las zonas relevantes para esa lectura."
        )
        subtitle.setObjectName("PageSubtitle")
        subtitle.setWordWrap(True)
        layout.addWidget(subtitle)

        filters_box = QGroupBox("Filtros")
        filters_layout = QHBoxLayout(filters_box)

        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Buscar barrio o distrito")
        self.search_input.textChanged.connect(self.load_data)

        self.window_combo = QComboBox()
        self.window_combo.addItems(["7", "14", "30"])
        self.window_combo.setCurrentText("14")
        self.window_combo.currentTextChanged.connect(self.load_data)

        self.limit_combo = QComboBox()
        self.limit_combo.addItems(["5", "10", "20"])
        self.limit_combo.setCurrentText("10")
        self.limit_combo.currentTextChanged.connect(self.load_data)

        self.open_map_button = QPushButton("Abrir selección en mapa")
        self.open_map_button.setObjectName("GhostButton")
        self.open_map_button.setEnabled(False)
        self.open_map_button.clicked.connect(self.open_selected_in_map)

        self.refresh_button = QPushButton("Actualizar")
        self.refresh_button.setObjectName("GhostButton")
        self.refresh_button.clicked.connect(self.load_data)

        filters_layout.addWidget(QLabel("Buscar"))
        filters_layout.addWidget(self.search_input, 1)
        filters_layout.addWidget(QLabel("Ventana"))
        filters_layout.addWidget(self.window_combo)
        filters_layout.addWidget(QLabel("Top"))
        filters_layout.addWidget(self.limit_combo)
        filters_layout.addWidget(self.open_map_button)
        filters_layout.addWidget(self.refresh_button)
        layout.addWidget(filters_box)

        self.summary_label = QLabel("Sin datos")
        self.summary_label.setObjectName("HeroSummary")
        self.summary_label.setWordWrap(True)
        layout.addWidget(self.summary_label)

        summary_grid = QGridLayout()
        summary_grid.setHorizontalSpacing(12)
        summary_grid.setVerticalSpacing(12)

        self.zones_total_card = StatCard("Zonas leídas")
        self.capture_card = StatCard("Oportunidad")
        self.heat_card = StatCard("Actividad")
        self.confidence_card = StatCard("Confianza")

        for idx, card in enumerate(
            [self.zones_total_card, self.capture_card, self.heat_card, self.confidence_card]
        ):
            summary_grid.addWidget(card, 0, idx)
        layout.addLayout(summary_grid)

        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        self.capture_table = RadarTable("Mejor captación", "Capture")
        self.heat_table = RadarTable("Más actividad", "Heat")
        self.transformation_table = RadarTable("Más transformación", "Transform")
        self.microzones_table = RadarTable("Microzonas", "Micro capture")
        self.low_conf_table = RadarTable("Confianza baja", "Confidence")

        self.tabs.addTab(self.capture_table, "Captación")
        self.tabs.addTab(self.heat_table, "Actividad")
        self.tabs.addTab(self.transformation_table, "Transformación")
        self.tabs.addTab(self.microzones_table, "Microzonas")
        self.tabs.addTab(self.low_conf_table, "Confianza")

        for table in (
            self.capture_table,
            self.heat_table,
            self.transformation_table,
            self.microzones_table,
            self.low_conf_table,
        ):
            table.row_selected.connect(self.on_table_row_selected)

        self.load_data()

    def _limit(self) -> int:
        return int(self.limit_combo.currentText())

    def _filter_rows(self, rows: list[dict]) -> list[dict]:
        query = (self.search_input.text() or "").strip().lower()
        if not query:
            return rows[: self._limit()]

        filtered = [
            row
            for row in rows
            if query in str(row.get("zone_label") or "").lower()
            or query in str(row.get("parent_zone_label") or "").lower()
        ]
        return filtered[: self._limit()]

    def on_table_row_selected(self, row: dict) -> None:
        self.selected_row_payload = row
        self.open_map_button.setEnabled(True)

    def open_selected_in_map(self) -> None:
        row = self.selected_row_payload
        if not row:
            return

        payload = {"window_days": int(self.window_combo.currentText())}
        if row.get("microzone_label"):
            payload["microzone_label"] = row.get("microzone_label")
            payload["zone_label"] = row.get("parent_zone_label") or row.get("zone_label")
        else:
            payload["zone_label"] = row.get("zone_label")

        self.open_in_map_requested.emit(payload)

    def load_data(self) -> None:
        window_days = int(self.window_combo.currentText())

        with SessionLocal() as session:
            payload = get_radar_payload_v2(session, window_days=window_days)

        summary = payload["summary"]
        self.summary_label.setText(
            f"Ventana {payload['window_days']}d. "
            f"Hay {summary['zones_total']} zonas leídas, "
            f"{summary['capture_ready_zones']} listas para captación y "
            f"{summary['low_confidence_zones']} con confianza baja."
        )

        self.zones_total_card.set_content(
            str(summary["zones_total"]),
            "barrios o distritos con lectura disponible",
        )
        self.capture_card.set_content(
            str(summary["capture_ready_zones"]),
            "zonas que hoy parecen más accionables",
        )
        self.heat_card.set_content(
            str(summary["hot_zones"]),
            "zonas con actividad fuerte",
        )
        self.confidence_card.set_content(
            str(summary["low_confidence_zones"]),
            "zonas donde todavía falta confianza o dato",
        )

        self.capture_table.load_rows(
            self._filter_rows(payload["top_capture"]),
            "zone_capture_score",
        )
        self.heat_table.load_rows(
            self._filter_rows(payload["top_heat"]),
            "zone_heat_score",
        )
        self.transformation_table.load_rows(
            self._filter_rows(payload["top_transformation"]),
            "zone_transformation_signal_score",
        )
        self.microzones_table.load_rows(
            self._filter_rows(payload["top_microzones"]),
            "microzone_capture_score",
        )
        self.low_conf_table.load_rows(
            self._filter_rows(payload["low_confidence"]),
            "zone_confidence_score",
        )
