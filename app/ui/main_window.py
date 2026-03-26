from PySide6.QtWidgets import QMainWindow, QTabWidget

from app.ui.views.assets_view import AssetsView
from app.ui.views.casafari_links_view import CasafariLinksView
from app.ui.views.dashboard_view import DashboardView
from app.ui.views.import_view import ImportView
from app.ui.views.opportunity_queue_view import OpportunityQueueView
from app.ui.views.radar_view import RadarView
from app.ui.views.sync_view import SyncView
from app.ui.views.zones_view import ZonesView


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Nexus Madrid")
        self.resize(1680, 1020)

        self.tabs = QTabWidget()
        self.tabs.addTab(DashboardView(), "Dashboard")
        self.tabs.addTab(ImportView(), "Importar CSV")
        self.tabs.addTab(CasafariLinksView(), "Casafari Links")
        self.tabs.addTab(AssetsView(), "Activos")
        self.tabs.addTab(ZonesView(), "Zonas")
        self.tabs.addTab(RadarView(), "Radar")
        self.tabs.addTab(OpportunityQueueView(), "Cola operativa")
        self.tabs.addTab(SyncView(), "Sync")

        self.setCentralWidget(self.tabs)