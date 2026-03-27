from __future__ import annotations

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QComboBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QScrollArea,
    QSplitter,
    QTabWidget,
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
    if value is None or value == "":
        return "-"
    return str(value)


def safe_money(value) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):,.0f} EUR".replace(",", ".")
    except (TypeError, ValueError):
        return str(value)


def safe_float(value, decimals: int = 1) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):.{decimals}f}"
    except (TypeError, ValueError):
        return str(value)


def safe_int(value) -> str:
    if value is None:
        return "-"
    try:
        return f"{int(round(float(value))):,}".replace(",", ".")
    except (TypeError, ValueError):
        return str(value)


def compact_detail_lines(*parts: str) -> str:
    return "\n".join(part for part in parts if part and part != "-")


class OpportunityQueueView(QWidget):
    open_in_map_requested = Signal(dict)

    def __init__(self) -> None:
        super().__init__()

        self.all_rows: list[dict] = []
        self.filtered_rows: list[dict] = []
        self.visible_rows: list[dict] = []
        self.group_rows: list[dict] = []
        self.selected_group_key: str | None = None
        self.selected_event_id: int | None = None
        self.selected_row_payload: dict | None = None

        root_layout = QVBoxLayout(self)
        root_layout.setContentsMargins(0, 0, 0, 0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        root_layout.addWidget(scroll)

        page = QWidget()
        page.setObjectName("PageScrollContainer")
        scroll.setWidget(page)
        layout = QVBoxLayout(page)
        layout.setContentsMargins(10, 8, 10, 20)
        layout.setSpacing(16)

        title = QLabel("Oportunidades")
        title.setObjectName("PageTitle")
        layout.addWidget(title)

        subtitle = QLabel(
            "Filtra primero. Luego abre el detalle del caso que realmente te interese. Lo avanzado queda fuera de la tabla principal."
        )
        subtitle.setObjectName("PageSubtitle")
        subtitle.setWordWrap(True)
        layout.addWidget(subtitle)

        filters_box = QGroupBox("Filtros principales")
        filters_layout = QVBoxLayout(filters_box)
        filters_layout.setSpacing(10)

        primary_filters = QHBoxLayout()
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Buscar por zona, dirección, portal o teléfono")
        self.search_input.textChanged.connect(self.apply_filters_and_render)

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

        self.score_combo = QComboBox()
        self.score_combo.addItems(["all", "40", "50", "60"])
        self.score_combo.currentTextChanged.connect(self.apply_filters_and_render)

        self.refresh_button = QPushButton("Actualizar")
        self.refresh_button.setObjectName("GhostButton")
        self.refresh_button.clicked.connect(self.load_data)

        primary_filters.addWidget(QLabel("Buscar"))
        primary_filters.addWidget(self.search_input, 1)
        primary_filters.addWidget(QLabel("Ventana"))
        primary_filters.addWidget(self.window_combo)
        primary_filters.addWidget(QLabel("Evento"))
        primary_filters.addWidget(self.event_combo)
        primary_filters.addWidget(QLabel("Score mínimo"))
        primary_filters.addWidget(self.score_combo)
        primary_filters.addWidget(self.refresh_button)
        filters_layout.addLayout(primary_filters)

        advanced_toggle_row = QHBoxLayout()
        self.toggle_advanced_button = QPushButton("Más filtros")
        self.toggle_advanced_button.setObjectName("GhostButton")
        self.toggle_advanced_button.clicked.connect(self.toggle_advanced_filters)
        advanced_toggle_row.addWidget(self.toggle_advanced_button)
        advanced_toggle_row.addStretch()
        filters_layout.addLayout(advanced_toggle_row)

        self.advanced_filters_box = QWidget()
        advanced_filters_layout = QHBoxLayout(self.advanced_filters_box)
        advanced_filters_layout.setContentsMargins(0, 0, 0, 0)
        advanced_filters_layout.setSpacing(12)

        self.geo_combo = QComboBox()
        self.geo_combo.addItems(["all", "with_geo", "without_geo"])
        self.geo_combo.currentTextChanged.connect(self.apply_filters_and_render)

        self.group_combo = QComboBox()
        self.group_combo.addItems(["none", "zone", "contact", "event_type"])
        self.group_combo.currentTextChanged.connect(self.on_group_mode_changed)

        advanced_filters_layout.addWidget(QLabel("Geo"))
        advanced_filters_layout.addWidget(self.geo_combo)
        advanced_filters_layout.addWidget(QLabel("Agrupar por"))
        advanced_filters_layout.addWidget(self.group_combo)
        advanced_filters_layout.addStretch()
        self.advanced_filters_box.setVisible(False)
        filters_layout.addWidget(self.advanced_filters_box)

        layout.addWidget(filters_box)

        self.summary_label = QLabel("Sin datos")
        self.summary_label.setObjectName("HeroSummary")
        self.summary_label.setWordWrap(True)
        layout.addWidget(self.summary_label)

        splitter = QSplitter()
        layout.addWidget(splitter, 1)

        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(12)

        self.groups_group = QGroupBox("Agrupación")
        groups_layout = QVBoxLayout(self.groups_group)
        self.groups_table = QTableWidget()
        self.groups_table.setColumnCount(5)
        self.groups_table.setHorizontalHeaderLabels(
            ["Grupo", "Casos", "Score top", "Último", "Motivo"]
        )
        self.groups_table.verticalHeader().setVisible(False)
        self.groups_table.itemSelectionChanged.connect(self.on_group_selected)
        groups_layout.addWidget(self.groups_table)
        self.groups_group.setVisible(False)
        left_layout.addWidget(self.groups_group)

        self.table = QTableWidget()
        self.table.setColumnCount(7)
        self.table.setHorizontalHeaderLabels(
            ["Score", "Señal", "Fecha", "Zona", "Portal", "Precio", "Resumen"]
        )
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        self.table.itemSelectionChanged.connect(self.on_selected)
        left_layout.addWidget(self.table, 1)

        splitter.addWidget(left_panel)

        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(12)

        detail_header = QHBoxLayout()
        self.detail_title = QLabel("Detalle")
        self.detail_title.setObjectName("SectionLabel")
        detail_header.addWidget(self.detail_title)
        detail_header.addStretch()

        self.open_map_button = QPushButton("Abrir en mapa")
        self.open_map_button.setObjectName("GhostButton")
        self.open_map_button.setEnabled(False)
        self.open_map_button.clicked.connect(self.open_selected_in_map)
        detail_header.addWidget(self.open_map_button)
        right_layout.addLayout(detail_header)

        self.detail_tabs = QTabWidget()
        right_layout.addWidget(self.detail_tabs)

        self._build_summary_tab()
        self._build_zone_tab()
        self._build_comps_tab()

        splitter.addWidget(right_panel)
        splitter.setSizes([960, 580])

        self.load_data()

    def _build_summary_tab(self) -> None:
        page = QWidget()
        page.setObjectName("PageScrollContainer")
        layout = QFormLayout(page)
        layout.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
        layout.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        layout.setLabelAlignment(Qt.AlignmentFlag.AlignTop)
        layout.setFormAlignment(Qt.AlignmentFlag.AlignTop)
        layout.setSpacing(12)

        self.lbl_score = QLabel("-")
        self.lbl_priority = QLabel("-")
        self.lbl_reason = QLabel("-")
        self.lbl_reason.setWordWrap(True)
        self.lbl_asset = QLabel("-")
        self.lbl_asset.setWordWrap(True)
        self.lbl_price = QLabel("-")
        self.lbl_price.setWordWrap(True)
        self.lbl_contact = QLabel("-")
        self.lbl_contact.setWordWrap(True)
        self.lbl_prediction = QLabel("-")
        self.lbl_prediction.setWordWrap(True)

        layout.addRow("Score", self.lbl_score)
        layout.addRow("Prioridad", self.lbl_priority)
        layout.addRow("Qué pasa", self.lbl_reason)
        layout.addRow("Activo", self.lbl_asset)
        layout.addRow("Precio", self.lbl_price)
        layout.addRow("Contacto", self.lbl_contact)
        layout.addRow("Lectura 30d", self.lbl_prediction)

        self.detail_tabs.addTab(page, "Resumen")

    def _build_zone_tab(self) -> None:
        page = QWidget()
        page.setObjectName("PageScrollContainer")
        layout = QFormLayout(page)
        layout.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
        layout.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        layout.setLabelAlignment(Qt.AlignmentFlag.AlignTop)
        layout.setFormAlignment(Qt.AlignmentFlag.AlignTop)
        layout.setSpacing(12)

        self.lbl_zone = QLabel("-")
        self.lbl_zone.setWordWrap(True)
        self.lbl_zone_context = QLabel("-")
        self.lbl_zone_context.setWordWrap(True)
        self.lbl_zone_scores = QLabel("-")
        self.lbl_zone_scores.setWordWrap(True)
        self.lbl_microzone = QLabel("-")
        self.lbl_microzone.setWordWrap(True)
        self.lbl_microzone_scores = QLabel("-")
        self.lbl_microzone_scores.setWordWrap(True)
        self.lbl_breakdown = QLabel("-")
        self.lbl_breakdown.setWordWrap(True)

        layout.addRow("Zona", self.lbl_zone)
        layout.addRow("Contexto", self.lbl_zone_context)
        layout.addRow("Scores zona", self.lbl_zone_scores)
        layout.addRow("Microzona", self.lbl_microzone)
        layout.addRow("Scores micro", self.lbl_microzone_scores)
        layout.addRow("Por qué sale arriba", self.lbl_breakdown)

        self.detail_tabs.addTab(page, "Zona")

    def _build_comps_tab(self) -> None:
        page = QWidget()
        layout = QVBoxLayout(page)

        self.comps_summary_label = QLabel("-")
        self.comps_summary_label.setWordWrap(True)
        layout.addWidget(self.comps_summary_label)

        self.comps_table = QTableWidget()
        self.comps_table.setColumnCount(5)
        self.comps_table.setHorizontalHeaderLabels(
            ["Asset", "Zona", "Tipo", "Precio", "Score"]
        )
        self.comps_table.verticalHeader().setVisible(False)
        layout.addWidget(self.comps_table)

        self.detail_tabs.addTab(page, "Comparables")

    def toggle_advanced_filters(self) -> None:
        is_visible = self.advanced_filters_box.isVisible()
        self.advanced_filters_box.setVisible(not is_visible)
        self.toggle_advanced_button.setText("Menos filtros" if not is_visible else "Más filtros")

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

        group_mode = self.group_combo.currentText()
        self.group_rows = build_opportunity_groups(
            self.filtered_rows,
            group_by=group_mode,
            limit=40,
        )
        self.groups_group.setVisible(group_mode != "none")
        self._render_groups()

        if self.selected_group_key and group_mode != "none":
            self.visible_rows = apply_group_selection(
                self.filtered_rows,
                group_by=group_mode,
                group_key=self.selected_group_key,
            )
        else:
            self.visible_rows = self.filtered_rows

        self._render_rows()
        self.summary_label.setText(
            f"{len(self.visible_rows)} oportunidades visibles. "
            f"Base actual: {len(self.all_rows)} | filtradas: {len(self.filtered_rows)}."
        )

    def _render_groups(self) -> None:
        self.groups_table.setRowCount(len(self.group_rows))
        for row_idx, row in enumerate(self.group_rows):
            values = [
                safe_text(row["group_label"]),
                safe_text(row["events_count"]),
                safe_text(row["top_score"]),
                safe_text(row["latest_event_datetime"]),
                safe_text(row["top_reason"]),
            ]
            for col_idx, value in enumerate(values):
                self.groups_table.setItem(row_idx, col_idx, QTableWidgetItem(value))
        self.groups_table.resizeColumnsToContents()

    def _render_rows(self) -> None:
        self.table.setRowCount(len(self.visible_rows))

        for row_idx, row in enumerate(self.visible_rows):
            values = [
                safe_text(row.get("score")),
                safe_text(row.get("event_type")),
                safe_text(row.get("event_datetime")),
                safe_text(row.get("zone_label")),
                safe_text(row.get("portal")),
                safe_money(row.get("price_new")),
                compact_detail_lines(
                    safe_text(row.get("priority_label")),
                    safe_text(row.get("ai_brief") or row.get("reason")),
                ),
            ]

            for col_idx, value in enumerate(values):
                item = QTableWidgetItem(value)
                if col_idx in (0, 5):
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
            f"Grupo activo: {len(self.visible_rows)} oportunidades dentro de la selección."
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
            detail = get_opportunity_detail_v2(
                session,
                event_id,
                window_days=self._window_days(),
            )

        if not detail.get("found"):
            self.clear_detail()
            self.detail_title.setText("Detalle no encontrado")
            return

        row = detail["queue_row"]
        self.selected_event_id = event_id
        self.selected_row_payload = row
        self.open_map_button.setEnabled(True)

        comps = detail.get("comparables") or {}
        comps_summary = comps.get("summary") or {}
        comps_rows = comps.get("comparables") or []

        self.detail_title.setText(f"Oportunidad #{event_id}")
        self.lbl_score.setText(safe_text(row.get("score")))
        self.lbl_priority.setText(safe_text(row.get("priority_label")))
        self.lbl_reason.setText(safe_text(row.get("ai_summary") or row.get("reason")))
        self.lbl_asset.setText(
            compact_detail_lines(
                safe_text(row.get("asset_address")),
                f"{safe_text(row.get('asset_type'))} | barrio {safe_text(row.get('asset_neighborhood'))}",
            )
        )
        self.lbl_price.setText(
            f"Nuevo: {safe_money(row.get('price_new'))} | anterior: {safe_money(row.get('price_old'))}"
        )
        self.lbl_contact.setText(
            compact_detail_lines(
                safe_text(row.get("contact_group_label")),
                f"Portal {safe_text(row.get('portal'))} | perfil {safe_text(row.get('phone_profile'))}",
            )
        )
        self.lbl_prediction.setText(
            compact_detail_lines(
                f"Oportunidad {safe_text(row.get('predicted_opportunity_30d_score'))} ({safe_text(row.get('predicted_opportunity_30d_band'))})",
                f"Zona {safe_text(row.get('predicted_absorption_30d_score'))} ({safe_text(row.get('predicted_absorption_30d_band'))})",
                safe_text(row.get("ai_next_step")),
            )
        )

        self.lbl_zone.setText(
            f"{safe_text(row.get('zone_label'))} | {safe_text(row.get('zone_recommended_action'))}"
        )
        self.lbl_zone_context.setText(
            compact_detail_lines(
                safe_text(row.get("ai_zone_context")),
                f"{safe_int(row.get('zone_population'))} hab | "
                f"{safe_float(row.get('zone_events_14d_per_10k_population'))} evt/10k | "
                f"IVT {safe_float(row.get('zone_vulnerability_index'))}",
            )
        )
        self.lbl_zone_scores.setText(
            compact_detail_lines(
                f"Captación {safe_text(row.get('zone_capture_score'))}",
                f"Heat relativo {safe_text(row.get('zone_relative_heat_score'))}",
                f"Transformación {safe_text(row.get('zone_transformation_signal_score'))}",
                f"Confianza {safe_text(row.get('zone_confidence_score'))}",
            )
        )
        self.lbl_microzone.setText(
            f"{safe_text(row.get('microzone_label'))} | {safe_text(row.get('microzone_recommended_action'))}"
        )
        self.lbl_microzone_scores.setText(
            compact_detail_lines(
                f"Captación {safe_text(row.get('microzone_capture_score'))}",
                f"Concentración {safe_text(row.get('microzone_concentration_score'))}",
                f"Confianza {safe_text(row.get('microzone_confidence_score'))}",
            )
        )
        self.lbl_breakdown.setText(
            compact_detail_lines(
                f"Evento {safe_text(row.get('score_event_base'))} | recencia {safe_text(row.get('score_recency'))}",
                f"Zona {safe_text(row.get('score_zone_signal'))} | microzona {safe_text(row.get('score_microzone_signal'))}",
                f"Geo {safe_text(row.get('score_geo_signal'))} | predicción {safe_text(row.get('score_predictive_signal'))}",
            )
        )

        self.lbl_breakdown.setText(
            compact_detail_lines(
                safe_text(row.get("ai_score_story")),
                f"Detalle tecnico: {safe_text(row.get('score_breakdown'))}",
            )
        )

        self.comps_summary_label.setText(
            f"Comparables: {safe_text(comps_summary.get('comparables_count'))} | "
            f"EUR/m2 medio: {safe_money(comps_summary.get('avg_comparable_price_m2'))}"
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
        self.selected_event_id = None
        self.selected_row_payload = None
        self.open_map_button.setEnabled(False)
        self.lbl_score.setText("-")
        self.lbl_priority.setText("-")
        self.lbl_reason.setText("-")
        self.lbl_asset.setText("-")
        self.lbl_price.setText("-")
        self.lbl_contact.setText("-")
        self.lbl_prediction.setText("-")
        self.lbl_zone.setText("-")
        self.lbl_zone_context.setText("-")
        self.lbl_zone_scores.setText("-")
        self.lbl_microzone.setText("-")
        self.lbl_microzone_scores.setText("-")
        self.lbl_breakdown.setText("-")
        self.comps_summary_label.setText("-")
        self.comps_table.setRowCount(0)

    def open_selected_in_map(self) -> None:
        row = self.selected_row_payload
        if not row or self.selected_event_id is None:
            return

        self.open_in_map_requested.emit(
            {
                "event_id": self.selected_event_id,
                "zone_label": row.get("zone_label"),
                "microzone_label": row.get("microzone_label"),
                "window_days": self._window_days(),
            }
        )
