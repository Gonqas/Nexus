from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QComboBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from core.services.opportunity_queue_detail_service_v2 import get_opportunity_detail_v2
from core.services.opportunity_queue_service_v2 import (
    apply_group_selection,
    build_opportunity_groups,
    filter_opportunity_rows,
    get_opportunity_queue_v2,
)
from db.session import SessionLocal


def safe_text(value) -> str:
    if value is None:
        return "-"
    return str(value)


def safe_money(value) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):,.0f} €".replace(",", ".")
    except (TypeError, ValueError):
        return str(value)


class OpportunityQueueView(QWidget):
    def __init__(self) -> None:
        super().__init__()

        self.all_rows: list[dict] = []
        self.filtered_rows: list[dict] = []
        self.visible_rows: list[dict] = []
        self.group_rows: list[dict] = []
        self.selected_group_key: str | None = None

        layout = QVBoxLayout(self)

        header_layout = QHBoxLayout()
        title = QLabel("Cola operativa")
        title.setStyleSheet("font-size: 22px; font-weight: bold;")

        subtitle = QLabel(
            "Eventos priorizados con filtros, agrupación operativa y detalle explicable para trabajo diario."
        )
        subtitle.setStyleSheet("color: #666;")
        subtitle.setWordWrap(True)

        self.refresh_button = QPushButton("Refrescar")
        self.refresh_button.clicked.connect(self.load_data)

        header_layout.addWidget(title)
        header_layout.addStretch()
        header_layout.addWidget(self.refresh_button)

        layout.addLayout(header_layout)
        layout.addWidget(subtitle)

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
        self.event_combo.currentTextChanged.connect(self.apply_filters_and_render)

        self.geo_combo = QComboBox()
        self.geo_combo.addItems(["all", "with_geo", "without_geo"])
        self.geo_combo.currentTextChanged.connect(self.apply_filters_and_render)

        self.score_combo = QComboBox()
        self.score_combo.addItems(["all", "40", "50", "60"])
        self.score_combo.currentTextChanged.connect(self.apply_filters_and_render)

        self.group_combo = QComboBox()
        self.group_combo.addItems(["none", "zone", "contact", "event_type"])
        self.group_combo.currentTextChanged.connect(self.on_group_mode_changed)

        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Filtrar por zona, dirección, portal o teléfono")
        self.search_input.textChanged.connect(self.apply_filters_and_render)

        controls.addWidget(QLabel("Ventana:"))
        controls.addWidget(self.window_combo)
        controls.addWidget(QLabel("Evento:"))
        controls.addWidget(self.event_combo)
        controls.addWidget(QLabel("Geo:"))
        controls.addWidget(self.geo_combo)
        controls.addWidget(QLabel("Score mín:"))
        controls.addWidget(self.score_combo)
        controls.addWidget(QLabel("Agrupar:"))
        controls.addWidget(self.group_combo)
        controls.addWidget(self.search_input, 1)
        layout.addLayout(controls)

        self.summary_label = QLabel("Sin datos")
        self.summary_label.setStyleSheet("color: #666;")
        self.summary_label.setWordWrap(True)
        layout.addWidget(self.summary_label)

        main_splitter = QSplitter()
        layout.addWidget(main_splitter)

        left_splitter = QSplitter(Qt.Orientation.Vertical)
        main_splitter.addWidget(left_splitter)

        self.groups_group = QGroupBox("Agrupación")
        groups_layout = QVBoxLayout(self.groups_group)
        self.groups_table = QTableWidget()
        self.groups_table.setColumnCount(6)
        self.groups_table.setHorizontalHeaderLabels(
            ["Grupo", "Eventos", "Top", "Media", "Último", "Motivo top"]
        )
        self.groups_table.verticalHeader().setVisible(False)
        self.groups_table.itemSelectionChanged.connect(self.on_group_selected)
        groups_layout.addWidget(self.groups_table)
        left_splitter.addWidget(self.groups_group)

        self.table = QTableWidget()
        self.table.setColumnCount(12)
        self.table.setHorizontalHeaderLabels(
            [
                "Score",
                "Prioridad",
                "Fecha",
                "Evento",
                "Zona",
                "Acción zona",
                "Portal",
                "Contacto",
                "Perfil tlf",
                "Geo",
                "Precio nuevo",
                "Motivo",
            ]
        )
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        self.table.itemSelectionChanged.connect(self.on_selected)
        left_splitter.addWidget(self.table)
        left_splitter.setSizes([240, 700])

        detail_widget = QWidget()
        detail_layout = QVBoxLayout(detail_widget)

        self.detail_title = QLabel("Detalle")
        self.detail_title.setStyleSheet("font-size: 20px; font-weight: bold;")
        detail_layout.addWidget(self.detail_title)

        self.summary_group = QGroupBox("Resumen")
        summary_form = QFormLayout(self.summary_group)
        self.lbl_score = QLabel("-")
        self.lbl_priority = QLabel("-")
        self.lbl_reason = QLabel("-")
        self.lbl_reason.setWordWrap(True)
        self.lbl_breakdown = QLabel("-")
        self.lbl_breakdown.setWordWrap(True)
        self.lbl_zone = QLabel("-")
        self.lbl_zone_capture = QLabel("-")
        self.lbl_zone_pressure = QLabel("-")
        self.lbl_zone_confidence = QLabel("-")
        self.lbl_asset = QLabel("-")
        self.lbl_asset.setWordWrap(True)
        self.lbl_geo = QLabel("-")
        self.lbl_price = QLabel("-")
        self.lbl_contact = QLabel("-")

        summary_form.addRow("Score", self.lbl_score)
        summary_form.addRow("Prioridad", self.lbl_priority)
        summary_form.addRow("Motivo", self.lbl_reason)
        summary_form.addRow("Breakdown", self.lbl_breakdown)
        summary_form.addRow("Zona", self.lbl_zone)
        summary_form.addRow("Capture zona", self.lbl_zone_capture)
        summary_form.addRow("Pressure zona", self.lbl_zone_pressure)
        summary_form.addRow("Confidence zona", self.lbl_zone_confidence)
        summary_form.addRow("Activo", self.lbl_asset)
        summary_form.addRow("Geo", self.lbl_geo)
        summary_form.addRow("Precio", self.lbl_price)
        summary_form.addRow("Contacto", self.lbl_contact)
        detail_layout.addWidget(self.summary_group)

        self.comps_group = QGroupBox("Comparables")
        comps_layout = QVBoxLayout(self.comps_group)
        self.comps_summary_label = QLabel("-")
        self.comps_summary_label.setWordWrap(True)
        comps_layout.addWidget(self.comps_summary_label)
        self.comps_table = QTableWidget()
        self.comps_table.setColumnCount(5)
        self.comps_table.setHorizontalHeaderLabels(
            ["Asset", "Zona", "Tipo", "Precio", "Score"]
        )
        self.comps_table.verticalHeader().setVisible(False)
        comps_layout.addWidget(self.comps_table)
        detail_layout.addWidget(self.comps_group)

        main_splitter.addWidget(detail_widget)
        main_splitter.setSizes([1100, 650])

        self.load_data()

    def _window_days(self) -> int:
        return int(self.window_combo.currentText())

    def _min_score(self) -> float | None:
        text = self.score_combo.currentText()
        if text == "all":
            return None
        return float(text)

    def load_data(self) -> None:
        self.selected_group_key = None

        with SessionLocal() as session:
            self.all_rows = get_opportunity_queue_v2(
                session,
                window_days=self._window_days(),
                limit=500,
            )

        self.apply_filters_and_render()

    def on_group_mode_changed(self) -> None:
        self.selected_group_key = None
        self.apply_filters_and_render()

    def apply_filters_and_render(self) -> None:
        self.filtered_rows = filter_opportunity_rows(
            self.all_rows,
            event_type_filter=self.event_combo.currentText(),
            geo_filter=self.geo_combo.currentText(),
            min_score=self._min_score(),
            zone_query=self.search_input.text(),
        )

        self.group_rows = build_opportunity_groups(
            self.filtered_rows,
            group_by=self.group_combo.currentText(),
            limit=40,
        )
        self._render_groups()

        if self.selected_group_key and self.group_combo.currentText() != "none":
            self.visible_rows = apply_group_selection(
                self.filtered_rows,
                group_by=self.group_combo.currentText(),
                group_key=self.selected_group_key,
            )
        else:
            self.visible_rows = self.filtered_rows

        self._render_rows()
        self.summary_label.setText(
            f"Eventos en ventana={len(self.all_rows)} | tras filtros={len(self.filtered_rows)} | "
            f"visibles={len(self.visible_rows)} | grupos={len(self.group_rows)}"
        )

    def _render_groups(self) -> None:
        self.groups_table.setRowCount(len(self.group_rows))
        for row_idx, row in enumerate(self.group_rows):
            values = [
                safe_text(row["group_label"]),
                safe_text(row["events_count"]),
                safe_text(row["top_score"]),
                safe_text(row["avg_score"]),
                safe_text(row["latest_event_datetime"]),
                safe_text(row["top_reason"]),
            ]
            for col_idx, value in enumerate(values):
                self.groups_table.setItem(row_idx, col_idx, QTableWidgetItem(value))
        self.groups_table.resizeColumnsToContents()

        if self.group_combo.currentText() == "none":
            self.groups_group.setTitle("Agrupación desactivada")
        else:
            self.groups_group.setTitle(f"Agrupación por {self.group_combo.currentText()}")

    def _render_rows(self) -> None:
        self.table.setRowCount(len(self.visible_rows))

        for row_idx, row in enumerate(self.visible_rows):
            values = [
                safe_text(row["score"]),
                safe_text(row["priority_label"]),
                safe_text(row["event_datetime"]),
                safe_text(row["event_type"]),
                safe_text(row["zone_label"]),
                safe_text(row["zone_recommended_action"]),
                safe_text(row["portal"]),
                safe_text(row["contact_group_label"]),
                safe_text(row["phone_profile"]),
                "Sí" if row["has_geo_point"] else "No",
                safe_money(row["price_new"]),
                safe_text(row["reason"]),
            ]

            for col_idx, value in enumerate(values):
                item = QTableWidgetItem(value)
                if col_idx in (0, 9, 10):
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.table.setItem(row_idx, col_idx, item)

        self.table.resizeColumnsToContents()

        if self.visible_rows:
            self.table.selectRow(0)
            self.load_detail(self.visible_rows[0]["event_id"])
        else:
            self.clear_detail()

    def on_group_selected(self) -> None:
        if self.group_combo.currentText() == "none":
            return

        items = self.groups_table.selectedItems()
        if not items:
            self.selected_group_key = None
            self.apply_filters_and_render()
            return

        row_idx = items[0].row()
        if row_idx < 0 or row_idx >= len(self.group_rows):
            return

        self.selected_group_key = self.group_rows[row_idx]["group_key"]
        self.visible_rows = apply_group_selection(
            self.filtered_rows,
            group_by=self.group_combo.currentText(),
            group_key=self.selected_group_key,
        )
        self._render_rows()
        self.summary_label.setText(
            f"Eventos en ventana={len(self.all_rows)} | tras filtros={len(self.filtered_rows)} | "
            f"grupo seleccionado={len(self.visible_rows)}"
        )

    def on_selected(self) -> None:
        items = self.table.selectedItems()
        if not items:
            return
        row_idx = items[0].row()
        if row_idx < 0 or row_idx >= len(self.visible_rows):
            return
        self.load_detail(self.visible_rows[row_idx]["event_id"])

    def load_detail(self, event_id: int) -> None:
        with SessionLocal() as session:
            detail = get_opportunity_detail_v2(session, event_id, window_days=self._window_days())

        if not detail.get("found"):
            self.clear_detail()
            self.detail_title.setText("Detalle no encontrado")
            return

        row = detail["queue_row"]
        comps = detail.get("comparables") or {}
        comps_summary = comps.get("summary") or {}
        comps_rows = comps.get("comparables") or []

        self.detail_title.setText(f"Oportunidad #{event_id}")
        self.lbl_score.setText(safe_text(row["score"]))
        self.lbl_priority.setText(safe_text(row["priority_label"]))
        self.lbl_reason.setText(safe_text(row["reason"]))
        self.lbl_breakdown.setText(safe_text(row["score_breakdown"]))
        self.lbl_zone.setText(
            f"{safe_text(row['zone_label'])} · {safe_text(row['zone_recommended_action'])}"
        )
        self.lbl_zone_capture.setText(safe_text(row["zone_capture_score"]))
        self.lbl_zone_pressure.setText(safe_text(row["zone_pressure_score"]))
        self.lbl_zone_confidence.setText(safe_text(row["zone_confidence_score"]))
        self.lbl_asset.setText(
            f"{safe_text(row['asset_address'])} · {safe_text(row['asset_type'])}"
        )
        self.lbl_geo.setText(
            f"barrio: {safe_text(row['asset_neighborhood'])} | "
            f"distrito: {safe_text(row['asset_district'])} | "
            f"coords: {'Sí' if row['has_geo_point'] else 'No'}"
        )
        self.lbl_price.setText(
            f"nuevo: {safe_money(row['price_new'])} | anterior: {safe_money(row['price_old'])}"
        )
        self.lbl_contact.setText(
            f"{safe_text(row['contact_group_label'])} | perfil: {safe_text(row['phone_profile'])}"
        )

        self.comps_summary_label.setText(
            f"comparables={safe_text(comps_summary.get('comparables_count'))} | "
            f"€/m² medio={safe_money(comps_summary.get('avg_comparable_price_m2'))} | "
            f"modo={'estricto' if comps_summary.get('used_strict_mode') else 'ampliado'}"
        )

        self.comps_table.setRowCount(len(comps_rows))
        for row_idx, comp in enumerate(comps_rows):
            values = [
                safe_text(comp.get("asset_id")),
                safe_text(comp.get("zone_label")),
                safe_text(comp.get("asset_type")),
                safe_money(comp.get("price_eur")),
                safe_text(comp.get("score")),
            ]
            for col_idx, value in enumerate(values):
                self.comps_table.setItem(row_idx, col_idx, QTableWidgetItem(value))
        self.comps_table.resizeColumnsToContents()

    def clear_detail(self) -> None:
        self.detail_title.setText("Detalle")
        self.lbl_score.setText("-")
        self.lbl_priority.setText("-")
        self.lbl_reason.setText("-")
        self.lbl_breakdown.setText("-")
        self.lbl_zone.setText("-")
        self.lbl_zone_capture.setText("-")
        self.lbl_zone_pressure.setText("-")
        self.lbl_zone_confidence.setText("-")
        self.lbl_asset.setText("-")
        self.lbl_geo.setText("-")
        self.lbl_price.setText("-")
        self.lbl_contact.setText("-")
        self.comps_summary_label.setText("-")
        self.comps_table.setRowCount(0)
