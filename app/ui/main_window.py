from __future__ import annotations

from dataclasses import dataclass

from PySide6.QtCore import Qt
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
        self.resize(1760, 1100)

        self.page_defs = [
            PageDef(
                "dashboard",
                "Panel maestro del nucleo",
                "Una lectura viva del baseline, matching, territorio y prediccion para operar sin perder contexto.",
                "Operacion",
                DashboardView,
            ),
            PageDef(
                "queue",
                "Cola operativa priorizada",
                "Oportunidades explicadas por zona, microzona y probabilidad de accion a corto plazo.",
                "Operacion",
                OpportunityQueueView,
            ),
            PageDef(
                "radar",
                "Radar territorial",
                "Lectura de calor, transformacion, microzonas y prediccion 30d lista para exploracion tactica.",
                "Operacion",
                RadarView,
            ),
            PageDef(
                "map",
                "Mapa operativo",
                "Explora oportunidades geolocalizadas y microzonas sobre el mapa para pasar de la lectura a la accion.",
                "Operacion",
                MapView,
            ),
            PageDef(
                "import",
                "Baseline vivo",
                "Sube y refresca la base maestra desde la app. El baseline cambia con el tiempo y la interfaz ya lo asume.",
                "Fuentes",
                ImportView,
            ),
            PageDef(
                "search",
                "Busqueda transversal",
                "Busca por direccion, telefono, portal, raw o reason taxonomy sin salir del flujo.",
                "Fuentes",
                SearchView,
            ),
            PageDef(
                "casafari",
                "Revision Casafari",
                "Audita matching, revisa links y corrige identidades cuando la automatizacion no llegue sola.",
                "Fuentes",
                CasafariLinksView,
            ),
            PageDef(
                "assets",
                "Inventario de activos",
                "Consulta el activo consolidado y su rastro de mercado desde el baseline hasta Casafari.",
                "Analisis",
                AssetsView,
            ),
            PageDef(
                "zones",
                "Zonas oficiales",
                "Explora la capa barrio y distrito como base de comparacion estable para todo lo demas.",
                "Analisis",
                ZonesView,
            ),
            PageDef(
                "sync",
                "Estado de sincronizacion",
                "Supervisa el pulso de las fuentes y controla la salud del ingest y del delta de mercado.",
                "Sistema",
                SyncView,
            ),
        ]

        self.page_widgets: list[QWidget] = [page.widget_cls() for page in self.page_defs]
        self.page_index_by_key = {page.key: idx for idx, page in enumerate(self.page_defs)}
        self.nav_buttons: list[QPushButton] = []

        shell = QWidget()
        shell.setObjectName("AppRoot")
        self.setCentralWidget(shell)

        shell_layout = QHBoxLayout(shell)
        shell_layout.setContentsMargins(0, 0, 0, 0)
        shell_layout.setSpacing(0)

        sidebar = self._build_sidebar()
        shell_layout.addWidget(sidebar)

        content = QWidget()
        content.setObjectName("ContentArea")
        content_layout = QVBoxLayout(content)
        content_layout.setContentsMargins(22, 18, 22, 18)
        content_layout.setSpacing(16)

        top_shell = self._build_top_shell()
        content_layout.addWidget(top_shell)

        self.stack = QStackedWidget()
        for widget in self.page_widgets:
            self.stack.addWidget(widget)
        content_layout.addWidget(self.stack, 1)

        shell_layout.addWidget(content, 1)

        self._wire_cross_navigation()

        self._activate_page(0)

    def _build_sidebar(self) -> QFrame:
        sidebar = QFrame()
        sidebar.setObjectName("Sidebar")
        sidebar.setFixedWidth(294)

        layout = QVBoxLayout(sidebar)
        layout.setContentsMargins(20, 20, 20, 18)
        layout.setSpacing(10)

        eyebrow = QLabel("Sistema operativo inmobiliario")
        eyebrow.setObjectName("BrandEyebrow")
        layout.addWidget(eyebrow)

        title = QLabel("Nexus Madrid")
        title.setObjectName("BrandTitle")
        layout.addWidget(title)

        subtitle = QLabel(
            "Baseline vivo, delta de mercado y lectura territorial en una sola consola."
        )
        subtitle.setObjectName("BrandSubtitle")
        subtitle.setWordWrap(True)
        layout.addWidget(subtitle)

        source_card = QFrame()
        source_card.setObjectName("SidebarCard")
        source_layout = QVBoxLayout(source_card)
        source_layout.setContentsMargins(16, 16, 16, 16)
        source_layout.setSpacing(6)

        source_badge = QLabel("Fuentes vivas")
        source_badge.setObjectName("TopBadge")
        source_layout.addWidget(source_badge, 0, Qt.AlignmentFlag.AlignLeft)

        source_text = QLabel(
            "El baseline se puede reimportar tantas veces como haga falta. Casafari y el resto del sistema deben seguir actualizandose encima."
        )
        source_text.setObjectName("SidebarFooter")
        source_text.setWordWrap(True)
        source_layout.addWidget(source_text)
        layout.addWidget(source_card)

        button_group = QButtonGroup(self)
        button_group.setExclusive(True)
        button_group.idClicked.connect(self._activate_page)

        current_group = None
        for idx, page in enumerate(self.page_defs):
            if page.nav_group != current_group:
                current_group = page.nav_group
                group_label = QLabel(current_group)
                group_label.setObjectName("NavSection")
                layout.addWidget(group_label)

            button = QPushButton(page.title)
            button.setObjectName("NavButton")
            button.setCheckable(True)
            button_group.addButton(button, idx)
            layout.addWidget(button)
            self.nav_buttons.append(button)

        layout.addStretch()

        footer = QLabel(
            "Preparado para el siguiente salto: mapas y trabajo territorial mas visual."
        )
        footer.setObjectName("SidebarFooter")
        footer.setWordWrap(True)
        layout.addWidget(footer)

        return sidebar

    def _build_top_shell(self) -> QFrame:
        shell = QFrame()
        shell.setObjectName("ShellHero")

        layout = QVBoxLayout(shell)
        layout.setContentsMargins(22, 20, 22, 20)
        layout.setSpacing(16)

        header = QHBoxLayout()
        header.setSpacing(14)

        title_box = QVBoxLayout()
        title_box.setSpacing(4)

        self.shell_title = QLabel("Nexus Madrid")
        self.shell_title.setObjectName("ShellTitle")
        title_box.addWidget(self.shell_title)

        self.shell_subtitle = QLabel("Sin seccion")
        self.shell_subtitle.setObjectName("ShellSubtitle")
        self.shell_subtitle.setWordWrap(True)
        title_box.addWidget(self.shell_subtitle)
        header.addLayout(title_box, 1)

        self.shell_badge = QLabel("Baseline vivo")
        self.shell_badge.setObjectName("TopBadge")
        header.addWidget(self.shell_badge, 0, Qt.AlignmentFlag.AlignTop)

        self.refresh_shell_button = QPushButton("Actualizar shell")
        self.refresh_shell_button.clicked.connect(self.refresh_shell_metrics)
        header.addWidget(self.refresh_shell_button, 0, Qt.AlignmentFlag.AlignTop)

        layout.addLayout(header)

        self.shell_hint = QLabel("")
        self.shell_hint.setObjectName("ShellHint")
        self.shell_hint.setWordWrap(True)
        layout.addWidget(self.shell_hint)

        metrics_row = QHBoxLayout()
        metrics_row.setSpacing(12)
        self.metric_assets = ShellMetricCard("Activos")
        self.metric_matching = ShellMetricCard("Matching")
        self.metric_geo = ShellMetricCard("Geo")
        self.metric_sync = ShellMetricCard("Ultimo sync")

        metrics_row.addWidget(self.metric_assets, 1)
        metrics_row.addWidget(self.metric_matching, 1)
        metrics_row.addWidget(self.metric_geo, 1)
        metrics_row.addWidget(self.metric_sync, 1)
        layout.addLayout(metrics_row)

        return shell

    def _wire_cross_navigation(self) -> None:
        map_view = self._get_page_widget("map", MapView)
        radar_view = self._get_page_widget("radar", RadarView)
        queue_view = self._get_page_widget("queue", OpportunityQueueView)

        if map_view and radar_view:
            radar_view.open_in_map_requested.connect(self._open_map_with_context)

        if map_view and queue_view:
            queue_view.open_in_map_requested.connect(self._open_map_with_context)

    def _get_page_widget(self, key: str, widget_type: type[QWidget]) -> QWidget | None:
        index = self.page_index_by_key.get(key)
        if index is None:
            return None
        widget = self.page_widgets[index]
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

    def _activate_page(self, index: int) -> None:
        self.stack.setCurrentIndex(index)
        page = self.page_defs[index]
        self.shell_title.setText(page.title)
        self.shell_subtitle.setText(page.subtitle)
        self.shell_hint.setText(
            f"Seccion activa: {page.nav_group}. Menos ruido, mas lectura util y lista para evolucionar con nuevos datos."
        )

        for idx, button in enumerate(self.nav_buttons):
            button.setChecked(idx == index)

        self.refresh_shell_metrics()

    def refresh_shell_metrics(self) -> None:
        try:
            with SessionLocal() as session:
                stats = get_dashboard_stats(session)
        except Exception as exc:
            self.metric_assets.set_content("-", "Sin lectura de base")
            self.metric_matching.set_content("-", "Sin lectura de matching")
            self.metric_geo.set_content("-", "Sin lectura geo")
            self.metric_sync.set_content("error", str(exc))
            return

        assets_total = int(stats.get("assets") or 0)
        raw_total = int(stats.get("casafari_raw") or 0)
        coords_total = int(stats.get("assets_with_geo_point") or 0)
        predictive_total = int(stats.get("predictive_zones_count") or 0)
        resolved_ratio = float(stats.get("casafari_resolved_ratio") or 0.0)
        sync_status = stats.get("last_sync_status") or "unknown"

        self.metric_assets.set_content(
            str(assets_total),
            f"{stats.get('buildings', 0)} edificios | {stats.get('listings', 0)} listings",
        )
        self.metric_matching.set_content(
            f"{resolved_ratio * 100:.1f}%",
            f"{stats.get('casafari_resolved', 0)}/{raw_total} raws | unresolved {stats.get('casafari_unresolved', 0)}",
        )
        self.metric_geo.set_content(
            f"{(coords_total / assets_total * 100):.1f}%" if assets_total else "0.0%",
            f"coords {coords_total}/{assets_total} | prediccion {predictive_total} zonas",
        )
        self.metric_sync.set_content(
            str(sync_status),
            f"items {stats.get('last_sync_item_count', 0)} | raw 7d {stats.get('casafari_raw_7d', 0)}",
        )
