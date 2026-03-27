from __future__ import annotations

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
    def __init__(self) -> None:
        super().__init__()

        root_layout = QVBoxLayout(self)
        root_layout.setContentsMargins(0, 0, 0, 0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        root_layout.addWidget(scroll)

        page = QWidget()
        scroll.setWidget(page)
        layout = QVBoxLayout(page)
        layout.setContentsMargins(10, 8, 10, 20)
        layout.setSpacing(16)

        title = QLabel("Buscar")
        title.setObjectName("PageTitle")
        layout.addWidget(title)

        subtitle = QLabel(
            "Haz una pregunta corta al dato y luego entra solo en el bloque que te interese. La búsqueda ya no necesita varias pantallas a la vez."
        )
        subtitle.setObjectName("PageSubtitle")
        subtitle.setWordWrap(True)
        layout.addWidget(subtitle)

        search_box = QGroupBox("Consulta")
        search_layout = QVBoxLayout(search_box)
        search_layout.setSpacing(10)

        first_row = QHBoxLayout()
        self.query_input = QLineEdit()
        self.query_input.setPlaceholderText(
            "Ejemplo: calle mayor 10, 699111222, weak_identity, fotocasa"
        )
        self.query_input.returnPressed.connect(self.run_search)

        self.section_filter = QComboBox()
        self.section_filter.addItems(["all", "assets", "listings", "raws", "events"])

        self.limit_combo = QComboBox()
        self.limit_combo.addItems(["10", "25", "50"])
        self.limit_combo.setCurrentText("25")

        self.search_button = QPushButton("Buscar")
        self.search_button.clicked.connect(self.run_search)

        self.reindex_button = QPushButton("Reindexar")
        self.reindex_button.setObjectName("GhostButton")
        self.reindex_button.clicked.connect(self.reindex_fts)

        first_row.addWidget(QLabel("Buscar"))
        first_row.addWidget(self.query_input, 1)
        first_row.addWidget(QLabel("Ámbito"))
        first_row.addWidget(self.section_filter)
        first_row.addWidget(QLabel("Límite"))
        first_row.addWidget(self.limit_combo)
        first_row.addWidget(self.search_button)
        first_row.addWidget(self.reindex_button)
        search_layout.addLayout(first_row)

        self.index_status_label = QLabel("FTS5 sin inicializar")
        self.index_status_label.setObjectName("MetricDetail")
        self.index_status_label.setWordWrap(True)
        search_layout.addWidget(self.index_status_label)
        layout.addWidget(search_box)

        self.summary_label = QLabel("Todavía no has lanzado ninguna búsqueda.")
        self.summary_label.setObjectName("HeroSummary")
        self.summary_label.setWordWrap(True)
        layout.addWidget(self.summary_label)

        self.tabs = QTabWidget()
        layout.addWidget(self.tabs, 1)

        self.assets_table = self._build_table_tab(
            "Activos",
            ["Asset", "Tipo", "Dirección", "Barrio", "Distrito", "Listings", "Snippet"],
        )
        self.listings_table = self._build_table_tab(
            "Listings",
            ["Listing", "Asset", "Portal", "Dirección", "Contacto", "Teléfono", "Precio", "Snippet"],
        )
        self.raws_table = self._build_table_tab(
            "Raw Casafari",
            ["Raw", "Fecha", "Estado", "Motivo", "Dirección", "Contacto", "Teléfono", "Portal", "Snippet"],
        )
        self.events_table = self._build_table_tab(
            "Eventos",
            ["Evento", "Fecha", "Tipo", "Canal", "Dirección", "Precio", "Snippet"],
        )

        self.refresh_index_status()

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
        with SessionLocal() as session:
            status = ensure_search_index(session, force_rebuild=force_rebuild)
            session.commit()

        self.index_status_label.setText(
            f"Índice {safe_text(status.get('backend'))} | documentos {safe_text(status.get('doc_count'))}"
        )

    def reindex_fts(self) -> None:
        self.refresh_index_status(force_rebuild=True)

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

        index_status = payload.get("index_status", {})
        self.index_status_label.setText(
            f"Índice {safe_text(index_status.get('backend'))} | documentos {safe_text(index_status.get('doc_count'))}"
        )

        summary = payload["summary"]
        self.summary_label.setText(
            f"Resultados para '{payload['query']}': {summary['total']} en total. "
            f"Activos {summary['assets']} | listings {summary['listings']} | raws {summary['raws']} | eventos {summary['events']}."
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
