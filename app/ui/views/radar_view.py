from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QGridLayout,
    QGroupBox,
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


class RadarTable(QGroupBox):
    def __init__(self, title: str, metric_label: str) -> None:
        super().__init__(title)

        self.metric_label = metric_label

        layout = QVBoxLayout(self)

        self.table = QTableWidget()
        self.table.setColumnCount(4)
        self.table.setHorizontalHeaderLabels(
            ["Zona", metric_label, "Confianza", "Acción"]
        )
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        layout.addWidget(self.table)

    def load_rows(self, rows: list[dict], metric_key: str) -> None:
        self.table.setRowCount(len(rows))

        for row_idx, row in enumerate(rows):
            values = [
                safe_text(row["zone_label"]),
                safe_text(row[metric_key]),
                safe_text(row["zone_confidence_score"]),
                safe_text(row["recommended_action"]),
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

        top_bar = QGridLayout()
        self.title = QLabel("Radar")
        self.title.setStyleSheet("font-size: 22px; font-weight: bold;")

        self.subtitle = QLabel("Rankings rápidos de captación, calor, presión, liquidez y confianza")
        self.subtitle.setStyleSheet("color: #666;")

        self.refresh_button = QPushButton("Refrescar")
        self.refresh_button.clicked.connect(self.load_data)

        top_bar.addWidget(self.title, 0, 0)
        top_bar.addWidget(self.refresh_button, 0, 1)
        top_bar.addWidget(self.subtitle, 1, 0, 1, 2)

        layout.addLayout(top_bar)

        grid = QGridLayout()
        self.capture_table = RadarTable("Top captación", "Capture")
        self.heat_table = RadarTable("Top calor", "Heat")
        self.pressure_table = RadarTable("Top presión", "Pressure")
        self.liquidity_table = RadarTable("Top liquidez", "Liquidity")
        self.low_conf_table = RadarTable("Baja confianza", "Confidence")

        grid.addWidget(self.capture_table, 0, 0)
        grid.addWidget(self.heat_table, 0, 1)
        grid.addWidget(self.pressure_table, 1, 0)
        grid.addWidget(self.liquidity_table, 1, 1)
        grid.addWidget(self.low_conf_table, 2, 0, 1, 2)

        layout.addLayout(grid)
        self.load_data()

    def load_data(self) -> None:
        with SessionLocal() as session:
            payload = get_radar_payload_v2(session, window_days=14)

        self.capture_table.load_rows(payload["top_capture"], "zone_capture_score")
        self.heat_table.load_rows(payload["top_heat"], "zone_heat_score")
        self.pressure_table.load_rows(payload["top_pressure"], "zone_pressure_score")
        self.liquidity_table.load_rows(payload["top_liquidity"], "zone_liquidity_score")
        self.low_conf_table.load_rows(payload["low_confidence"], "zone_confidence_score")