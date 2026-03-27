from __future__ import annotations

from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QComboBox,
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

from core.services.copilot_service import run_copilot_query
from core.services.search_service import ensure_search_index, search_payload
from db.session import SessionLocal


def safe_text(value: object | None) -> str:
    if value is None or value == "":
        return "-"
    return str(value)


def safe_money(value: object | None) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):,.0f} EUR".replace(",", ".")
    except (TypeError, ValueError):
        return str(value)


class SearchView(QWidget):
    open_context_requested = Signal(dict)
    open_map_requested = Signal(dict)
    execute_action_requested = Signal(dict)

    def __init__(self) -> None:
        super().__init__()
        self._has_loaded = False
        self.copilot_rows: list[dict] = []

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

        title = QLabel("Copiloto")
        title.setObjectName("PageTitle")
        layout.addWidget(title)

        subtitle = QLabel(
            "Pregunta que quieres saber o hacer. El copiloto responde, abre contexto o ejecuta acciones sin obligarte a recorrer menus."
        )
        subtitle.setObjectName("PageSubtitle")
        subtitle.setWordWrap(True)
        layout.addWidget(subtitle)

        query_box = QGroupBox("Pregunta o pide una accion")
        query_layout = QVBoxLayout(query_box)
        query_layout.setSpacing(10)

        first_row = QHBoxLayout()
        self.query_input = QLineEdit()
        self.query_input.setPlaceholderText(
            "Ejemplo: zonas para captar, casafari weak identity, sincroniza casafari"
        )
        self.query_input.returnPressed.connect(self.run_copilot)

        self.limit_combo = QComboBox()
        self.limit_combo.addItems(["5", "10", "25"])
        self.limit_combo.setCurrentText("10")

        self.copilot_button = QPushButton("Preguntar")
        self.copilot_button.clicked.connect(self.run_copilot)

        self.classic_toggle_button = QPushButton("Mostrar busqueda clasica")
        self.classic_toggle_button.setObjectName("GhostButton")
        self.classic_toggle_button.clicked.connect(self.toggle_classic_results)

        first_row.addWidget(QLabel("Tu pregunta"))
        first_row.addWidget(self.query_input, 1)
        first_row.addWidget(QLabel("Top"))
        first_row.addWidget(self.limit_combo)
        first_row.addWidget(self.copilot_button)
        first_row.addWidget(self.classic_toggle_button)
        query_layout.addLayout(first_row)

        prompt_row = QHBoxLayout()
        prompt_row.addWidget(QLabel("Preguntas rapidas"))
        for label, query in (
            ("Zonas para captar", "zonas para captar"),
            ("Oportunidades hoy", "oportunidades con entrada nueva"),
            ("Casafari dudoso", "casafari weak identity"),
            ("Sincronizar Casafari", "sincroniza casafari"),
        ):
            button = QPushButton(label)
            button.setObjectName("GhostButton")
            button.clicked.connect(lambda _checked=False, q=query: self.run_preset_query(q))
            prompt_row.addWidget(button)
        prompt_row.addStretch()
        query_layout.addLayout(prompt_row)

        self.index_status_label = QLabel("Indice sin inicializar")
        self.index_status_label.setObjectName("MetricDetail")
        self.index_status_label.setWordWrap(True)
        query_layout.addWidget(self.index_status_label)
        layout.addWidget(query_box)

        self.summary_label = QLabel(
            "Empieza por una frase corta. El copiloto puede explicar, navegar, abrir el mapa o lanzar acciones."
        )
        self.summary_label.setObjectName("HeroSummary")
        self.summary_label.setWordWrap(True)
        layout.addWidget(self.summary_label)

        self.copilot_box = QGroupBox("Respuesta")
        copilot_layout = QVBoxLayout(self.copilot_box)
        copilot_layout.setSpacing(8)

        self.copilot_title_label = QLabel("Todavia no has preguntado al copiloto.")
        self.copilot_title_label.setObjectName("SectionLabel")
        copilot_layout.addWidget(self.copilot_title_label)

        self.copilot_answer_label = QLabel(
            "Prueba con zonas, oportunidades, Casafari o acciones del sistema. Por ejemplo: 'zonas con transformacion' o 'preparar sesion casafari'."
        )
        self.copilot_answer_label.setWordWrap(True)
        copilot_layout.addWidget(self.copilot_answer_label)

        self.copilot_next_label = QLabel("-")
        self.copilot_next_label.setObjectName("MetricDetail")
        self.copilot_next_label.setWordWrap(True)
        copilot_layout.addWidget(self.copilot_next_label)

        self.copilot_table = QTableWidget(0, 4)
        self.copilot_table.setHorizontalHeaderLabels(["Tipo", "Item", "Por que", "Accion"])
        self.copilot_table.verticalHeader().setVisible(False)
        self.copilot_table.setAlternatingRowColors(True)
        self.copilot_table.setCornerButtonEnabled(False)
        self.copilot_table.setMinimumHeight(220)
        self.copilot_table.itemSelectionChanged.connect(self.on_copilot_row_selected)
        copilot_layout.addWidget(self.copilot_table)

        self.execute_action_button = QPushButton("Ejecutar accion")
        self.execute_action_button.setObjectName("GhostButton")
        self.execute_action_button.setEnabled(False)
        self.execute_action_button.clicked.connect(self.execute_selected_action)

        self.open_context_button = QPushButton("Abrir contexto")
        self.open_context_button.setObjectName("GhostButton")
        self.open_context_button.setEnabled(False)
        self.open_context_button.clicked.connect(self.open_selected_context)

        self.open_map_button = QPushButton("Abrir en mapa")
        self.open_map_button.setObjectName("GhostButton")
        self.open_map_button.setEnabled(False)
        self.open_map_button.clicked.connect(self.open_selected_in_map)

        buttons_row = QHBoxLayout()
        buttons_row.addWidget(self.execute_action_button)
        buttons_row.addWidget(self.open_context_button)
        buttons_row.addWidget(self.open_map_button)
        buttons_row.addStretch()
        copilot_layout.addLayout(buttons_row)
        layout.addWidget(self.copilot_box)

        self.classic_results_box = QGroupBox("Busqueda clasica y resultados")
        classic_layout = QVBoxLayout(self.classic_results_box)
        classic_layout.setSpacing(10)

        classic_hint = QLabel(
            "Usa esta parte solo cuando necesites explorar resultados en bruto o cuando el copiloto caiga a busqueda general."
        )
        classic_hint.setObjectName("MetricDetail")
        classic_hint.setWordWrap(True)
        classic_layout.addWidget(classic_hint)

        classic_row = QHBoxLayout()
        self.section_filter = QComboBox()
        self.section_filter.addItems(["all", "assets", "listings", "raws", "events"])

        self.search_button = QPushButton("Lanzar busqueda clasica")
        self.search_button.clicked.connect(self.run_search)

        self.reindex_button = QPushButton("Reindexar")
        self.reindex_button.setObjectName("GhostButton")
        self.reindex_button.clicked.connect(self.reindex_fts)

        classic_row.addWidget(QLabel("Ambito"))
        classic_row.addWidget(self.section_filter)
        classic_row.addWidget(self.search_button)
        classic_row.addWidget(self.reindex_button)
        classic_row.addStretch()
        classic_layout.addLayout(classic_row)

        self.tabs = QTabWidget()
        classic_layout.addWidget(self.tabs, 1)
        layout.addWidget(self.classic_results_box, 1)

        self.assets_table = self._build_table_tab(
            "Activos",
            ["Asset", "Tipo", "Direccion", "Barrio", "Distrito", "Listings", "Snippet"],
        )
        self.listings_table = self._build_table_tab(
            "Listings",
            ["Listing", "Asset", "Portal", "Direccion", "Contacto", "Telefono", "Precio", "Snippet"],
        )
        self.raws_table = self._build_table_tab(
            "Raw Casafari",
            ["Raw", "Fecha", "Estado", "Motivo", "Direccion", "Contacto", "Telefono", "Portal", "Snippet"],
        )
        self.events_table = self._build_table_tab(
            "Eventos",
            ["Evento", "Fecha", "Tipo", "Canal", "Direccion", "Precio", "Snippet"],
        )

        self.show_classic_results(False)

    def ensure_loaded(self, *, force: bool = False) -> None:
        if self._has_loaded and not force:
            return
        self.refresh_index_status(force_rebuild=force)

    def run_preset_query(self, query: str) -> None:
        self.query_input.setText(query)
        self.run_copilot()

    def toggle_classic_results(self) -> None:
        self.show_classic_results(self.classic_results_box.isHidden())

    def show_classic_results(self, visible: bool) -> None:
        self.classic_results_box.setVisible(visible)
        if visible:
            self.classic_toggle_button.setText("Ocultar busqueda clasica")
        else:
            self.classic_toggle_button.setText("Mostrar busqueda clasica")

    def _build_table_tab(self, title: str, headers: list[str]) -> QTableWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        table = QTableWidget(0, len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.verticalHeader().setVisible(False)
        table.setAlternatingRowColors(True)
        table.setCornerButtonEnabled(False)
        layout.addWidget(table)
        self.tabs.addTab(page, title)
        return table

    def refresh_index_status(self, *, force_rebuild: bool = False) -> None:
        self._has_loaded = True
        with SessionLocal() as session:
            status = ensure_search_index(session, force_rebuild=force_rebuild)
            session.commit()

        self.index_status_label.setText(
            f"Indice {safe_text(status.get('backend'))} | documentos {safe_text(status.get('doc_count'))}"
        )

    def reindex_fts(self) -> None:
        self.refresh_index_status(force_rebuild=True)

    def on_copilot_row_selected(self) -> None:
        row_idx = self.copilot_table.currentRow()
        enabled = 0 <= row_idx < len(self.copilot_rows)
        self.open_context_button.setEnabled(enabled)
        can_execute = False
        can_open_map = False
        if enabled:
            row = self.copilot_rows[row_idx]
            can_execute = bool(row.get("action_id"))
            can_open_map = bool(
                row.get("zone_label") or row.get("microzone_label") or row.get("event_id")
            )
        self.execute_action_button.setEnabled(can_execute)
        self.open_map_button.setEnabled(can_open_map)

    def open_selected_context(self) -> None:
        row_idx = self.copilot_table.currentRow()
        if row_idx < 0 or row_idx >= len(self.copilot_rows):
            return
        payload = dict(self.copilot_rows[row_idx])
        self.open_context_requested.emit(payload)

    def open_selected_in_map(self) -> None:
        row_idx = self.copilot_table.currentRow()
        if row_idx < 0 or row_idx >= len(self.copilot_rows):
            return
        row = self.copilot_rows[row_idx]
        payload = {
            "zone_label": row.get("zone_label"),
            "microzone_label": row.get("microzone_label"),
            "event_id": row.get("event_id"),
            "window_days": 14,
        }
        self.open_map_requested.emit(payload)

    def execute_selected_action(self) -> None:
        row_idx = self.copilot_table.currentRow()
        if row_idx < 0 or row_idx >= len(self.copilot_rows):
            return
        row = dict(self.copilot_rows[row_idx])
        if not row.get("action_id"):
            return
        self.execute_action_requested.emit(row)

    def run_copilot(self) -> None:
        query = self.query_input.text().strip()
        limit = int(self.limit_combo.currentText())

        with SessionLocal() as session:
            payload = run_copilot_query(session, query=query, default_limit=limit)
            session.commit()

        self.copilot_title_label.setText(safe_text(payload.get("title")))
        self.copilot_answer_label.setText(safe_text(payload.get("answer")))
        self.copilot_next_label.setText(f"Siguiente paso: {safe_text(payload.get('next_step'))}")
        self.summary_label.setText(
            f"Copiloto activo sobre '{safe_text(payload.get('query'))}' con lectura {safe_text(payload.get('intent'))}."
        )

        suggestions = payload.get("suggestions") or []
        self.copilot_rows = list(suggestions)
        self.copilot_table.setRowCount(len(suggestions))
        for row_idx, row in enumerate(suggestions):
            values = [
                safe_text(row.get("tipo")),
                safe_text(row.get("item")),
                safe_text(row.get("por_que")),
                safe_text(row.get("accion")),
            ]
            for col_idx, value in enumerate(values):
                self.copilot_table.setItem(row_idx, col_idx, QTableWidgetItem(value))
        self.copilot_table.resizeColumnsToContents()

        if suggestions:
            self.copilot_table.selectRow(0)
            self.on_copilot_row_selected()
        else:
            self.execute_action_button.setEnabled(False)
            self.open_context_button.setEnabled(False)
            self.open_map_button.setEnabled(False)

        search_result = payload.get("search_payload")
        if search_result:
            self.show_classic_results(True)
            self._apply_search_payload(search_result, query_override="Busqueda general")

    def run_search(self) -> None:
        query = self.query_input.text().strip()
        section_filter = self.section_filter.currentText()
        limit = int(self.limit_combo.currentText())

        with SessionLocal() as session:
            payload = search_payload(
                session,
                query=query,
                section_filter=section_filter,
                limit_per_section=limit,
            )
            session.commit()

        self.show_classic_results(True)
        self._apply_search_payload(payload)

    def _apply_search_payload(self, payload: dict, *, query_override: str | None = None) -> None:
        index_status = payload.get("index_status", {})
        self.index_status_label.setText(
            f"Indice {safe_text(index_status.get('backend'))} | documentos {safe_text(index_status.get('doc_count'))}"
        )

        summary = payload["summary"]
        query_label = query_override or payload["query"]
        self.summary_label.setText(
            f"{query_label}: {summary['total']} resultados. Activos {summary['assets']} | listings {summary['listings']} | raws {summary['raws']} | eventos {summary['events']}."
        )

        self._load_assets(payload["assets"])
        self._load_listings(payload["listings"])
        self._load_raws(payload["raws"])
        self._load_events(payload["events"])

    def _load_assets(self, rows: list[dict]) -> None:
        self.assets_table.setRowCount(len(rows))
        for row_idx, row in enumerate(rows):
            values = [
                safe_text(row.get("asset_id")),
                safe_text(row.get("asset_type")),
                safe_text(row.get("address")),
                safe_text(row.get("neighborhood")),
                safe_text(row.get("district")),
                safe_text(row.get("listings_count")),
                safe_text(row.get("snippet")),
            ]
            for col_idx, value in enumerate(values):
                self.assets_table.setItem(row_idx, col_idx, QTableWidgetItem(value))
        self.assets_table.resizeColumnsToContents()

    def _load_listings(self, rows: list[dict]) -> None:
        self.listings_table.setRowCount(len(rows))
        for row_idx, row in enumerate(rows):
            values = [
                safe_text(row.get("listing_id")),
                safe_text(row.get("asset_id")),
                safe_text(row.get("portal")),
                safe_text(row.get("address")),
                safe_text(row.get("contact_name")),
                safe_text(row.get("contact_phone")),
                safe_money(row.get("price_eur")),
                safe_text(row.get("snippet")),
            ]
            for col_idx, value in enumerate(values):
                self.listings_table.setItem(row_idx, col_idx, QTableWidgetItem(value))
        self.listings_table.resizeColumnsToContents()

    def _load_raws(self, rows: list[dict]) -> None:
        self.raws_table.setRowCount(len(rows))
        for row_idx, row in enumerate(rows):
            values = [
                safe_text(row.get("raw_history_item_id")),
                safe_text(row.get("event_datetime")),
                safe_text(row.get("match_status")),
                safe_text(row.get("reason_taxonomy")),
                safe_text(row.get("address")),
                safe_text(row.get("contact_name")),
                safe_text(row.get("contact_phone")),
                safe_text(row.get("portal")),
                safe_text(row.get("snippet")),
            ]
            for col_idx, value in enumerate(values):
                self.raws_table.setItem(row_idx, col_idx, QTableWidgetItem(value))
        self.raws_table.resizeColumnsToContents()

    def _load_events(self, rows: list[dict]) -> None:
        self.events_table.setRowCount(len(rows))
        for row_idx, row in enumerate(rows):
            values = [
                safe_text(row.get("market_event_id")),
                safe_text(row.get("event_datetime")),
                safe_text(row.get("event_type")),
                safe_text(row.get("source_channel")),
                safe_text(row.get("address")),
                safe_money(row.get("price_new")),
                safe_text(row.get("snippet")),
            ]
            for col_idx, value in enumerate(values):
                self.events_table.setItem(row_idx, col_idx, QTableWidgetItem(value))
        self.events_table.resizeColumnsToContents()
