from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QComboBox,
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

from core.services.radar_service_v2 import get_radar_payload_v2
from db.session import SessionLocal


def safe_text(value) -> str:
    if value is None:
        return "-"
    return str(value)


class StatCard(QGroupBox):
    def __init__(self, title: str, value: str, detail: str = "") -> None:
        super().__init__(title)
        layout = QVBoxLayout(self)

        self.value_label = QLabel(value)
        self.value_label.setStyleSheet("font-size: 24px; font-weight: bold;")
        layout.addWidget(self.value_label)

        self.detail_label = QLabel(detail)
        self.detail_label.setStyleSheet("color: #666;")
        self.detail_label.setWordWrap(True)
        layout.addWidget(self.detail_label)

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
        layout.addWidget(self.table)

    def load_rows(self, rows: list[dict], metric_key: str) -> None:
        self.table.setRowCount(len(rows))

        for row_idx, row in enumerate(rows):
            confidence_value = row.get("zone_confidence_score")
            if confidence_value is None:
                confidence_value = row.get("microzone_confidence_score")

            values = [
                safe_text(row["zone_label"]),
                safe_text(row[metric_key]),
                safe_text(confidence_value),
                safe_text(row["recommended_action"]),
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

        layout = QVBoxLayout(self)

        header = QHBoxLayout()
        title_box = QVBoxLayout()

        self.title = QLabel("Radar")
        self.title.setStyleSheet("font-size: 22px; font-weight: bold;")
        title_box.addWidget(self.title)

        self.subtitle = QLabel(
            "Lectura rapida de captacion, calor, presion, liquidez y confianza con ventanas 7/14/30 y normalizacion por poblacion cuando hay contexto oficial."
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

        summary_grid.addWidget(self.zones_total_card, 0, 0)
        summary_grid.addWidget(self.capture_ready_card, 0, 1)
        summary_grid.addWidget(self.high_conf_card, 0, 2)
        summary_grid.addWidget(self.low_conf_card, 1, 0)
        summary_grid.addWidget(self.hot_zones_card, 1, 1)
        summary_grid.addWidget(self.relative_hot_card, 1, 2)
        summary_grid.addWidget(self.transform_card, 2, 0)
        summary_grid.addWidget(self.predictive_card, 2, 1)
        summary_grid.addWidget(self.microzones_card, 2, 2)
        summary_grid.addWidget(self.microzone_hotspots_card, 3, 0)
        layout.addLayout(summary_grid)

        grid = QGridLayout()
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
            f"Ventana={payload['window_days']}d | zonas={summary['zones_total']} | "
            f"capture ready={summary['capture_ready_zones']} | "
            f"alta confianza={summary['high_confidence_zones']} | "
            f"baja confianza={summary['low_confidence_zones']} | "
            f"zonas calientes={summary['hot_zones']} | "
            f"hotspots relativos={summary.get('relative_hot_zones', 0)} | "
            f"zonas transformacion={summary.get('transform_zones', 0)} | "
            f"prediccion 30d={summary.get('predictive_zones', 0)} | "
            f"microzonas={summary.get('microzones_total', 0)} | "
            f"microhotspots={summary.get('microzone_hotspots', 0)}"
        )

        self.zones_total_card.set_value(str(summary["zones_total"]))
        self.zones_total_card.set_detail(f"ventana {payload['window_days']}d")
        self.capture_ready_card.set_value(str(summary["capture_ready_zones"]))
        self.capture_ready_card.set_detail("capture>=60 y confidence>=50")
        self.high_conf_card.set_value(str(summary["high_confidence_zones"]))
        self.high_conf_card.set_detail("confidence>=60")
        self.low_conf_card.set_value(str(summary["low_confidence_zones"]))
        self.low_conf_card.set_detail("confidence<40")
        self.hot_zones_card.set_value(str(summary["hot_zones"]))
        self.hot_zones_card.set_detail("heat>=65")
        self.relative_hot_card.set_value(str(summary.get("relative_hot_zones", 0)))
        self.relative_hot_card.set_detail("relative_heat>=65")
        self.transform_card.set_value(str(summary.get("transform_zones", 0)))
        self.transform_card.set_detail("transformation>=65")
        self.predictive_card.set_value(str(summary.get("predictive_zones", 0)))
        self.predictive_card.set_detail("pred_absorption_30d>=65")
        self.microzones_card.set_value(str(summary.get("microzones_total", 0)))
        self.microzones_card.set_detail("celdas activas con geo")
        self.microzone_hotspots_card.set_value(str(summary.get("microzone_hotspots", 0)))
        self.microzone_hotspots_card.set_detail("micro capture>=65")

        self.capture_table.load_rows(payload["top_capture"], "zone_capture_score")
        self.heat_table.load_rows(payload["top_heat"], "zone_heat_score")
        self.pressure_table.load_rows(payload["top_pressure"], "zone_pressure_score")
        self.transformation_table.load_rows(payload["top_transformation"], "zone_transformation_signal_score")
        self.liquidity_table.load_rows(payload["top_liquidity"], "zone_liquidity_score")
        self.predictive_table.load_rows(payload["top_predictive"], "predicted_absorption_30d_score")
        self.low_conf_table.load_rows(payload["low_confidence"], "zone_confidence_score")
        self.microzones_table.load_rows(payload["top_microzones"], "microzone_capture_score")
