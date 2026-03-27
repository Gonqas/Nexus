from __future__ import annotations

from dataclasses import dataclass
from time import monotonic

from PySide6.QtCore import Qt, QTimer
from PySide6.QtWidgets import (
    QButtonGroup,
    QFrame,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QPushButton,
    QStackedWidget,
    QVBoxLayout,
    QWidget,
)

from app.ui.views.assets_view import AssetsView
from app.ui.views.casafari_links_view import CasafariLinksView
from app.ui.views.dashboard_view import DashboardView
from app.ui.views.import_view import ImportView
from app.ui.views.map_view import MapView
from app.ui.views.opportunity_queue_view import OpportunityQueueView
from app.ui.views.radar_view import RadarView
from app.ui.views.search_view import SearchView
from app.ui.views.sync_view import SyncView
from app.ui.views.zones_view import ZonesView
from db.repositories.dashboard_repo import get_dashboard_stats
from db.session import SessionLocal


@dataclass(frozen=True)
class PageDef:
    key: str
    title: str
    subtitle: str
    nav_group: str
    widget_cls: type[QWidget]


class ShellMetricCard(QFrame):
    def __init__(self, title: str) -> None:
        super().__init__()
        self.setObjectName("ShellMetricCard")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 14, 16, 14)
        layout.setSpacing(4)

        self.title_label = QLabel(title)
        self.title_label.setObjectName("ShellMetricTitle")
        layout.addWidget(self.title_label)

        self.value_label = QLabel("-")
        self.value_label.setObjectName("ShellMetricValue")
        layout.addWidget(self.value_label)

        self.detail_label = QLabel("")
        self.detail_label.setObjectName("MetricDetail")
        self.detail_label.setWordWrap(True)
        layout.addWidget(self.detail_label)

    def set_content(self, value: str, detail: str) -> None:
        self.value_label.setText(value)
        self.detail_label.setText(detail)


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Nexus Madrid")
        self.resize(1720, 1080)

        self.page_defs = [
            PageDef(
                "search",
                "Copiloto",
                "Pregunta, pide una accion o abre contexto sin tener que navegar por toda la herramienta.",
                "Entrada principal",
                SearchView,
            ),
            PageDef(
                "queue",
                "Oportunidades",
                "Lista priorizada para decidir que revisar o mover hoy.",
                "Trabajo diario",
                OpportunityQueueView,
            ),
            PageDef(
                "radar",
                "Zonas",
                "Lectura territorial simple para detectar donde hay senal y donde no.",
                "Trabajo diario",
                RadarView,
            ),
            PageDef(
                "map",
                "Mapa",
                "Contexto espacial para pasar de la lista a una lectura de zona rapida.",
                "Trabajo diario",
                MapView,
            ),
            PageDef(
                "casafari",
                "Casafari",
                "Revision del matching y de los casos que todavia no quedan claros.",
                "Trabajo diario",
                CasafariLinksView,
            ),
            PageDef(
                "dashboard",
                "Resumen",
                "Vista corta del estado del sistema: base, Casafari, geografia y senales clave.",
                "Herramientas",
                DashboardView,
            ),
            PageDef(
                "import",
                "Datos base",
                "Sube o reimporta el baseline desde la app cuando cambie la base real.",
                "Herramientas",
                ImportView,
            ),
            PageDef(
                "sync",
                "Sincronizacion",
                "Control de salud del scraping, login y cobertura del ultimo delta.",
                "Herramientas",
                SyncView,
            ),
            PageDef(
                "assets",
                "Activos",
                "Consulta de activos consolidados y su rastro de mercado.",
                "Explorar",
                AssetsView,
            ),
            PageDef(
                "zones",
                "Barrios y distritos",
                "Lectura estable de zonas oficiales para comparar resultados.",
                "Explorar",
                ZonesView,
            ),
        ]

        self.page_index_by_key = {page.key: index for index, page in enumerate(self.page_defs)}
        self.page_widgets: list[QWidget | None] = [None] * len(self.page_defs)
        self.page_placeholders: list[QWidget] = []
        self.nav_buttons: list[QPushButton] = []
        self._shell_metrics_cache: dict | None = None
        self._shell_metrics_loaded_at = 0.0
        self._shell_metrics_ttl_s = 5.0

        app_root = QWidget()
        app_root.setObjectName("AppRoot")
        self.setCentralWidget(app_root)

        root_layout = QHBoxLayout(app_root)
        root_layout.setContentsMargins(0, 0, 0, 0)
        root_layout.setSpacing(0)

        sidebar = self._build_sidebar()
        root_layout.addWidget(sidebar, 0)

        content = QWidget()
        content.setObjectName("ContentArea")
        content_layout = QVBoxLayout(content)
        content_layout.setContentsMargins(18, 16, 18, 18)
        content_layout.setSpacing(16)

        self.top_shell = self._build_top_shell()
        content_layout.addWidget(self.top_shell, 0)

        self.stack = QStackedWidget()
        content_layout.addWidget(self.stack, 1)
        root_layout.addWidget(content, 1)

        for page in self.page_defs:
            placeholder = self._build_placeholder(page)
            self.page_placeholders.append(placeholder)
            self.stack.addWidget(placeholder)

        self._activate_page(0)

    def _build_sidebar(self) -> QFrame:
        sidebar = QFrame()
        sidebar.setObjectName("Sidebar")
        sidebar.setFixedWidth(286)

        layout = QVBoxLayout(sidebar)
        layout.setContentsMargins(16, 18, 16, 18)
        layout.setSpacing(14)

        eyebrow = QLabel("NEXUS MADRID")
        eyebrow.setObjectName("BrandEyebrow")
        layout.addWidget(eyebrow)

        title = QLabel("Copiloto operativo")
        title.setObjectName("BrandTitle")
        layout.addWidget(title)

        subtitle = QLabel(
            "Usa la IA como entrada principal. Lo visual queda para validar, comparar o bajar al mapa."
        )
        subtitle.setObjectName("BrandSubtitle")
        subtitle.setWordWrap(True)
        layout.addWidget(subtitle)

        prompt_card = QFrame()
        prompt_card.setObjectName("SidebarCard")
        prompt_layout = QVBoxLayout(prompt_card)
        prompt_layout.setContentsMargins(14, 14, 14, 14)
        prompt_layout.setSpacing(10)

        prompt_title = QLabel("Empieza aqui")
        prompt_title.setObjectName("SectionLabel")
        prompt_layout.addWidget(prompt_title)

        prompt_text = QLabel(
            "Pregunta cosas como 'zonas para captar', 'casafari weak identity' o 'sincroniza Casafari'."
        )
        prompt_text.setObjectName("MetricDetail")
        prompt_text.setWordWrap(True)
        prompt_layout.addWidget(prompt_text)

        for label, query in (
            ("Captacion hoy", "zonas para captar"),
            ("Casafari a revisar", "casafari weak identity"),
            ("Sincronizar Casafari", "sincroniza casafari"),
        ):
            button = QPushButton(label)
            button.setObjectName("GhostButton")
            button.clicked.connect(lambda _checked=False, q=query: self._launch_copilot_prompt(q))
            prompt_layout.addWidget(button)

        layout.addWidget(prompt_card)

        self.nav_button_group = QButtonGroup(self)
        self.nav_button_group.setExclusive(True)

        primary_keys = {"search", "queue", "radar", "map", "casafari"}
        sections = [
            ("Principal", [page for page in self.page_defs if page.key in primary_keys]),
            ("Herramientas", [page for page in self.page_defs if page.key not in primary_keys]),
        ]

        for section_title, pages in sections:
            section_label = QLabel(section_title)
            section_label.setObjectName("NavSection")
            layout.addWidget(section_label)
            for page in pages:
                button = QPushButton(page.title)
                button.setObjectName("NavButton")
                button.setCheckable(True)
                button.clicked.connect(
                    lambda _checked=False, idx=self.page_index_by_key[page.key]: self._activate_page(idx)
                )
                self.nav_button_group.addButton(button)
                self.nav_buttons.append(button)
                layout.addWidget(button)

        layout.addStretch(1)

        footer = QLabel(
            "Menos paneles abiertos a la vez. Pregunta primero y baja a detalle solo cuando ya tienes foco."
        )
        footer.setObjectName("SidebarFooter")
        footer.setWordWrap(True)
        layout.addWidget(footer)
        return sidebar

    def _build_top_shell(self) -> QFrame:
        shell = QFrame()
        shell.setObjectName("ShellHero")

        layout = QVBoxLayout(shell)
        layout.setContentsMargins(20, 18, 20, 18)
        layout.setSpacing(14)

        header = QHBoxLayout()
        header.setSpacing(12)

        title_box = QVBoxLayout()
        title_box.setSpacing(2)

        self.shell_title = QLabel("Copiloto")
        self.shell_title.setObjectName("ShellTitle")
        title_box.addWidget(self.shell_title)

        self.shell_subtitle = QLabel("")
        self.shell_subtitle.setObjectName("ShellSubtitle")
        self.shell_subtitle.setWordWrap(True)
        title_box.addWidget(self.shell_subtitle)
        header.addLayout(title_box, 1)

        self.shell_badge = QLabel("Entrada principal")
        self.shell_badge.setObjectName("TopBadge")
        header.addWidget(self.shell_badge, 0, Qt.AlignmentFlag.AlignTop)

        self.refresh_shell_button = QPushButton("Actualizar")
        self.refresh_shell_button.setObjectName("GhostButton")
        self.refresh_shell_button.clicked.connect(lambda: self.refresh_shell_metrics(force=True))
        header.addWidget(self.refresh_shell_button, 0, Qt.AlignmentFlag.AlignTop)
        layout.addLayout(header)

        self.shell_hint = QLabel("")
        self.shell_hint.setObjectName("ShellHint")
        self.shell_hint.setWordWrap(True)
        layout.addWidget(self.shell_hint)

        metrics_row = QHBoxLayout()
        metrics_row.setSpacing(12)
        self.metric_base = ShellMetricCard("Base")
        self.metric_market = ShellMetricCard("Casafari")
        self.metric_coverage = ShellMetricCard("Cobertura")
        self.metric_sync = ShellMetricCard("Estado")

        metrics_row.addWidget(self.metric_base, 1)
        metrics_row.addWidget(self.metric_market, 1)
        metrics_row.addWidget(self.metric_coverage, 1)
        metrics_row.addWidget(self.metric_sync, 1)
        layout.addLayout(metrics_row)

        return shell

    def _build_placeholder(self, page: PageDef) -> QWidget:
        placeholder = QWidget()
        placeholder.setObjectName("PageScrollContainer")

        layout = QVBoxLayout(placeholder)
        layout.setContentsMargins(24, 24, 24, 24)
        layout.setSpacing(10)

        title = QLabel(page.title)
        title.setObjectName("PageTitle")
        layout.addWidget(title)

        subtitle = QLabel(page.subtitle)
        subtitle.setObjectName("PageSubtitle")
        subtitle.setWordWrap(True)
        layout.addWidget(subtitle)

        hint = QLabel("Cargando modulo...")
        hint.setObjectName("MetricDetail")
        layout.addWidget(hint)

        layout.addStretch(1)
        return placeholder

    def _launch_copilot_prompt(self, query: str) -> None:
        search_index = self.page_index_by_key.get("search")
        if search_index is None:
            return

        self._activate_page(search_index)
        search_view = self._get_page_widget("search", SearchView)
        if search_view is None:
            return
        search_view.ensure_loaded()
        search_view.run_preset_query(query)

    def _ensure_page(self, index: int) -> QWidget:
        existing = self.page_widgets[index]
        if existing is not None:
            return existing

        widget = self.page_defs[index].widget_cls()
        placeholder = self.page_placeholders[index]
        self.stack.removeWidget(placeholder)
        placeholder.deleteLater()
        self.stack.insertWidget(index, widget)
        self.page_widgets[index] = widget

        page_key = self.page_defs[index].key
        if page_key == "radar" and isinstance(widget, RadarView):
            widget.open_in_map_requested.connect(self._open_map_with_context)
        if page_key == "queue" and isinstance(widget, OpportunityQueueView):
            widget.open_in_map_requested.connect(self._open_map_with_context)
        if page_key == "search" and isinstance(widget, SearchView):
            widget.open_context_requested.connect(self._open_copilot_context)
            widget.open_map_requested.connect(self._open_map_with_context)
            widget.execute_action_requested.connect(self._execute_copilot_action)

        return widget

    def _get_page_widget(self, key: str, widget_type: type[QWidget]) -> QWidget | None:
        index = self.page_index_by_key.get(key)
        if index is None:
            return None
        widget = self._ensure_page(index)
        if isinstance(widget, widget_type):
            return widget
        return None

    def _open_map_with_context(self, payload: dict) -> None:
        map_index = self.page_index_by_key.get("map")
        if map_index is None:
            return

        self._activate_page(map_index)
        map_view = self._get_page_widget("map", MapView)
        if map_view is None:
            return

        map_view.focus_context(
            zone_label=payload.get("zone_label"),
            microzone_label=payload.get("microzone_label"),
            event_id=payload.get("event_id"),
            window_days=payload.get("window_days"),
        )

    def _open_copilot_context(self, payload: dict) -> None:
        target_view = str(payload.get("target_view") or "").strip().lower()

        if target_view == "radar":
            page_index = self.page_index_by_key.get("radar")
            if page_index is None:
                return
            self._activate_page(page_index)
            radar_view = self._get_page_widget("radar", RadarView)
            if radar_view is not None:
                radar_view.focus_context(
                    zone_label=payload.get("zone_label"),
                    window_days=14,
                )
            return

        if target_view == "queue":
            page_index = self.page_index_by_key.get("queue")
            if page_index is None:
                return
            self._activate_page(page_index)
            queue_view = self._get_page_widget("queue", OpportunityQueueView)
            if queue_view is not None:
                queue_view.focus_context(
                    event_id=payload.get("event_id"),
                    zone_label=payload.get("zone_label"),
                    microzone_label=payload.get("microzone_label"),
                    window_days=14,
                )
            return

        if target_view == "casafari":
            page_index = self.page_index_by_key.get("casafari")
            if page_index is None:
                return
            self._activate_page(page_index)
            casafari_view = self._get_page_widget("casafari", CasafariLinksView)
            if casafari_view is not None:
                casafari_view.focus_context(
                    query_text=payload.get("query_text"),
                    focus_filter=payload.get("focus_filter"),
                )
            return

        if payload.get("zone_label") or payload.get("microzone_label") or payload.get("event_id"):
            self._open_map_with_context(
                {
                    "zone_label": payload.get("zone_label"),
                    "microzone_label": payload.get("microzone_label"),
                    "event_id": payload.get("event_id"),
                    "window_days": 14,
                }
            )

    def _execute_copilot_action(self, payload: dict) -> None:
        action_id = str(payload.get("action_id") or "").strip().lower()

        if action_id == "search_reindex":
            page_index = self.page_index_by_key.get("search")
            if page_index is None:
                return
            self._activate_page(page_index)
            search_view = self._get_page_widget("search", SearchView)
            if search_view is not None:
                search_view.ensure_loaded()
                search_view.reindex_fts()
            return

        if action_id == "casafari_prepare_session":
            page_index = self.page_index_by_key.get("sync")
            if page_index is None:
                return
            self._activate_page(page_index)
            sync_view = self._get_page_widget("sync", SyncView)
            if sync_view is not None:
                sync_view.ensure_loaded()
                sync_view.start_prepare_session()
            return

        if action_id == "casafari_sync":
            page_index = self.page_index_by_key.get("sync")
            if page_index is None:
                return
            self._activate_page(page_index)
            sync_view = self._get_page_widget("sync", SyncView)
            if sync_view is not None:
                sync_view.ensure_loaded()
                sync_view.start_sync()
            return

        if action_id == "casafari_reconcile":
            page_index = self.page_index_by_key.get("casafari")
            if page_index is None:
                return
            self._activate_page(page_index)
            casafari_view = self._get_page_widget("casafari", CasafariLinksView)
            if casafari_view is not None:
                casafari_view.refresh_all()
                casafari_view.start_rerun()

    def _page_hint(self, key: str) -> str:
        hints = {
            "search": "Empieza por una pregunta corta. Si hace falta bajar a detalle, el copiloto te llevara al modulo correcto.",
            "queue": "Filtra primero y abre detalle solo sobre las oportunidades que ya pintan accionables.",
            "radar": "Compara pocas zonas a la vez. Usa el mapa cuando necesites contexto espacial y deja fuera lo accesorio.",
            "map": "Valida en mapa lo que ya has detectado con el copiloto o con el radar, no al reves.",
            "casafari": "Usa Casafari para revisar solo los casos dudosos o relanzar acciones operativas.",
        }
        return hints.get(
            key,
            "La navegacion secundaria queda como apoyo. Si una duda es transversal, vuelve al copiloto.",
        )

    def _activate_page(self, index: int) -> None:
        self._ensure_page(index)
        self.stack.setCurrentIndex(index)
        page = self.page_defs[index]
        self.shell_title.setText(page.title)
        self.shell_subtitle.setText(page.subtitle)
        self.shell_badge.setText(page.nav_group)
        self.shell_hint.setText(self._page_hint(page.key))

        for idx, button in enumerate(self.nav_buttons):
            button.setChecked(idx == index)

        self.refresh_shell_metrics()
        QTimer.singleShot(0, lambda idx=index: self._ensure_page_loaded(idx))

    def _ensure_page_loaded(self, index: int) -> None:
        widget = self.page_widgets[index]
        if widget is None:
            return
        ensure_loaded = getattr(widget, "ensure_loaded", None)
        if callable(ensure_loaded):
            ensure_loaded()

    def refresh_shell_metrics(self, *, force: bool = False) -> None:
        now = monotonic()
        if (
            not force
            and self._shell_metrics_cache is not None
            and (now - self._shell_metrics_loaded_at) < self._shell_metrics_ttl_s
        ):
            stats = self._shell_metrics_cache
        else:
            try:
                with SessionLocal() as session:
                    stats = get_dashboard_stats(session)
            except Exception as exc:
                self.metric_base.set_content("-", "Sin lectura de base")
                self.metric_market.set_content("-", "Sin lectura de mercado")
                self.metric_coverage.set_content("-", "Sin lectura geografica")
                self.metric_sync.set_content("error", str(exc))
                return
            self._shell_metrics_cache = stats
            self._shell_metrics_loaded_at = now

        assets_total = int(stats.get("assets") or 0)
        coords_total = int(stats.get("assets_with_geo_point") or 0)
        raw_total = int(stats.get("casafari_raw") or 0)
        resolved_total = int(stats.get("casafari_resolved") or 0)
        sync_status = str(stats.get("last_sync_status") or "sin dato")
        coords_pct = f"{(coords_total / assets_total * 100):.1f}%" if assets_total else "0.0%"
        matching_pct = f"{(resolved_total / raw_total * 100):.1f}%" if raw_total else "0.0%"

        self.metric_base.set_content(
            str(assets_total),
            f"{stats.get('buildings', 0)} edificios y {stats.get('listings', 0)} listings",
        )
        self.metric_market.set_content(
            matching_pct,
            f"{resolved_total} casos claros de {raw_total} raws",
        )
        self.metric_coverage.set_content(
            coords_pct,
            f"{coords_total} activos con coordenadas",
        )
        self.metric_sync.set_content(
            sync_status,
            f"{stats.get('last_sync_item_count', 0)} items en el ultimo sync",
        )
