from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QComboBox,
    QFrame,
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

from core.services.radar_service_v2 import get_radar_payload_v2
from db.session import SessionLocal


def safe_text(value) -> str:
    if value is None:
        return "-"
    return str(value)


class StatCard(QGroupBox):
    def __init__(self, title: str, value: str, detail: str = "") -> None:
        super().__init__(title)
        self.setMinimumHeight(108)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 14, 16, 14)
        layout.setSpacing(4)

        self.value_label = QLabel(value)
        self.value_label.setStyleSheet("font-size: 28px; font-weight: bold;")
        layout.addWidget(self.value_label)

        self.detail_label = QLabel(detail)
        self.detail_label.setStyleSheet("color: #666;")
        self.detail_label.setWordWrap(True)
        layout.addWidget(self.detail_label)
        layout.addStretch()

    def set_value(self, value: str) -> None:
        self.value_label.setText(value)

    def set_detail(self, detail: str) -> None:
        self.detail_label.setText(detail)


class RadarTable(QGroupBox):
    def __init__(self, title: str, metric_label: str) -> None:
        super().__init__(title)

        layout = QVBoxLayout(self)
        self.table = QTableWidget()
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(
            ["Zona", metric_label, "Confianza", "Accion", "Explicacion"]
        )
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        self.table.setCornerButtonEnabled(False)
        self.table.setMinimumHeight(220)
        layout.addWidget(self.table)

    def load_rows(self, rows: list[dict], metric_key: str) -> None:
        self.table.setRowCount(len(rows))

        for row_idx, row in enumerate(rows):
            confidence_value = row.get("zone_confidence_score")
            if confidence_value is None:
                confidence_value = row.get("microzone_confidence_score")

            values = [
                safe_text(row["zone_label"]),
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


class RadarView(QWidget):
    def __init__(self) -> None:
        super().__init__()

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

        self.title = QLabel("Radar")
        self.title.setStyleSheet("font-size: 22px; font-weight: bold;")
        title_box.addWidget(self.title)

        self.subtitle = QLabel(
            "Lectura territorial compacta para captar rapido: actividad, presion, transformacion, prediccion y microzonas."
        )
        self.subtitle.setStyleSheet("color: #666;")
        self.subtitle.setWordWrap(True)
        title_box.addWidget(self.subtitle)

        controls = QHBoxLayout()
        self.window_combo = QComboBox()
        self.window_combo.addItems(["7", "14", "30"])
        self.window_combo.setCurrentText("14")
        self.window_combo.currentTextChanged.connect(self.load_data)

        self.refresh_button = QPushButton("Refrescar")
        self.refresh_button.clicked.connect(self.load_data)

        controls.addWidget(QLabel("Ventana:"))
        controls.addWidget(self.window_combo)
        controls.addWidget(self.refresh_button)

        header.addLayout(title_box)
        header.addStretch()
        header.addLayout(controls)
        layout.addLayout(header)

        self.summary_label = QLabel("Sin datos")
        self.summary_label.setStyleSheet("color: #666;")
        self.summary_label.setWordWrap(True)
        layout.addWidget(self.summary_label)

        summary_grid = QGridLayout()
        summary_grid.setHorizontalSpacing(12)
        summary_grid.setVerticalSpacing(12)

        self.zones_total_card = StatCard("Zonas", "0")
        self.capture_ready_card = StatCard("Capture ready", "0")
        self.high_conf_card = StatCard("Alta confianza", "0")
        self.low_conf_card = StatCard("Baja confianza", "0")
        self.hot_zones_card = StatCard("Zonas calientes", "0")
        self.relative_hot_card = StatCard("Hotspots relativos", "0")
        self.transform_card = StatCard("Zonas transformacion", "0")
        self.predictive_card = StatCard("Prediccion 30d", "0")
        self.microzones_card = StatCard("Microzonas", "0")
        self.microzone_hotspots_card = StatCard("Microhotspots", "0")

        cards = [
            self.zones_total_card,
            self.capture_ready_card,
            self.high_conf_card,
            self.low_conf_card,
            self.hot_zones_card,
            self.relative_hot_card,
            self.transform_card,
            self.predictive_card,
            self.microzones_card,
            self.microzone_hotspots_card,
        ]
        for idx, card in enumerate(cards):
            summary_grid.addWidget(card, idx // 3, idx % 3)
        layout.addLayout(summary_grid)

        grid = QGridLayout()
        grid.setHorizontalSpacing(12)
        grid.setVerticalSpacing(12)

        self.capture_table = RadarTable("Top captacion", "Capture")
        self.heat_table = RadarTable("Top calor", "Heat")
        self.pressure_table = RadarTable("Top presion", "Pressure")
        self.transformation_table = RadarTable("Top transformacion", "Transform")
        self.liquidity_table = RadarTable("Top liquidez", "Liquidity")
        self.predictive_table = RadarTable("Top prediccion 30d", "Pred 30d")
        self.low_conf_table = RadarTable("Baja confianza", "Confidence")
        self.microzones_table = RadarTable("Top microzonas", "Micro capture")

        grid.addWidget(self.capture_table, 0, 0)
        grid.addWidget(self.heat_table, 0, 1)
        grid.addWidget(self.pressure_table, 1, 0)
        grid.addWidget(self.transformation_table, 1, 1)
        grid.addWidget(self.liquidity_table, 2, 0)
        grid.addWidget(self.predictive_table, 2, 1)
        grid.addWidget(self.low_conf_table, 3, 0)
        grid.addWidget(self.microzones_table, 3, 1)
        layout.addLayout(grid)

        self.load_data()

    def load_data(self) -> None:
        window_days = int(self.window_combo.currentText())

        with SessionLocal() as session:
            payload = get_radar_payload_v2(session, window_days=window_days)

        summary = payload["summary"]
        self.summary_label.setText(
            f"{summary['zones_total']} zonas leidas en {payload['window_days']} dias. "
            f"Capture ready: {summary['capture_ready_zones']}. "
            f"Hotspots relativos: {summary.get('relative_hot_zones', 0)}. "
            f"Transformacion: {summary.get('transform_zones', 0)}. "
            f"Prediccion 30d: {summary.get('predictive_zones', 0)}. "
            f"Microzonas activas: {summary.get('microzones_total', 0)}."
        )

        self.zones_total_card.set_value(str(summary["zones_total"]))
        self.zones_total_card.set_detail(f"ventana {payload['window_days']}d")
        self.capture_ready_card.set_value(str(summary["capture_ready_zones"]))
        self.capture_ready_card.set_detail("captacion lista")
        self.high_conf_card.set_value(str(summary["high_confidence_zones"]))
        self.high_conf_card.set_detail("confidence >= 60")
        self.low_conf_card.set_value(str(summary["low_confidence_zones"]))
        self.low_conf_card.set_detail("confidence < 40")
        self.hot_zones_card.set_value(str(summary["hot_zones"]))
        self.hot_zones_card.set_detail("heat >= 65")
        self.relative_hot_card.set_value(str(summary.get("relative_hot_zones", 0)))
        self.relative_hot_card.set_detail("actividad relativa")
        self.transform_card.set_value(str(summary.get("transform_zones", 0)))
        self.transform_card.set_detail("senal transformadora")
        self.predictive_card.set_value(str(summary.get("predictive_zones", 0)))
        self.predictive_card.set_detail("mejor lectura 30d")
        self.microzones_card.set_value(str(summary.get("microzones_total", 0)))
        self.microzones_card.set_detail("celdas con geo")
        self.microzone_hotspots_card.set_value(str(summary.get("microzone_hotspots", 0)))
        self.microzone_hotspots_card.set_detail("microzonas fuertes")

        self.capture_table.load_rows(payload["top_capture"], "zone_capture_score")
        self.heat_table.load_rows(payload["top_heat"], "zone_heat_score")
        self.pressure_table.load_rows(payload["top_pressure"], "zone_pressure_score")
        self.transformation_table.load_rows(
            payload["top_transformation"], "zone_transformation_signal_score"
        )
        self.liquidity_table.load_rows(payload["top_liquidity"], "zone_liquidity_score")
        self.predictive_table.load_rows(
            payload["top_predictive"], "predicted_absorption_30d_score"
        )
        self.low_conf_table.load_rows(payload["low_confidence"], "zone_confidence_score")
        self.microzones_table.load_rows(payload["top_microzones"], "microzone_capture_score")
