from PySide6.QtWidgets import (
    QGridLayout,
    QGroupBox,
    QLabel,
    QVBoxLayout,
    QWidget,
)

from db.repositories.dashboard_repo import get_dashboard_stats
from db.session import SessionLocal


class StatCard(QGroupBox):
    def __init__(self, title: str, value: str) -> None:
        super().__init__(title)

        layout = QVBoxLayout(self)

        self.value_label = QLabel(value)
        self.value_label.setStyleSheet("font-size: 28px; font-weight: bold;")
        layout.addWidget(self.value_label)

    def set_value(self, value: str) -> None:
        self.value_label.setText(value)


class DashboardView(QWidget):
    def __init__(self) -> None:
        super().__init__()

        self.layout = QVBoxLayout(self)

        title = QLabel("Dashboard")
        title.setStyleSheet("font-size: 22px; font-weight: bold;")
        self.layout.addWidget(title)

        self.grid = QGridLayout()
        self.layout.addLayout(self.grid)

        self.assets_card = StatCard("Activos", "0")
        self.buildings_card = StatCard("Edificios", "0")
        self.contacts_card = StatCard("Contactos", "0")
        self.listings_card = StatCard("Listings", "0")
        self.events_card = StatCard("Eventos", "0")
        self.casafari_raw_card = StatCard("Raw Casafari", "0")
        self.casafari_resolved_card = StatCard("Casafari resueltos", "0")
        self.casafari_ambiguous_card = StatCard("Casafari ambiguos", "0")
        self.casafari_unresolved_card = StatCard("Casafari sin resolver", "0")
        self.casafari_events_card = StatCard("Eventos Casafari", "0")

        self.grid.addWidget(self.assets_card, 0, 0)
        self.grid.addWidget(self.buildings_card, 0, 1)
        self.grid.addWidget(self.contacts_card, 0, 2)
        self.grid.addWidget(self.listings_card, 1, 0)
        self.grid.addWidget(self.events_card, 1, 1)
        self.grid.addWidget(self.casafari_raw_card, 1, 2)
        self.grid.addWidget(self.casafari_resolved_card, 2, 0)
        self.grid.addWidget(self.casafari_ambiguous_card, 2, 1)
        self.grid.addWidget(self.casafari_unresolved_card, 2, 2)
        self.grid.addWidget(self.casafari_events_card, 3, 0)

        self.refresh()

    def refresh(self) -> None:
        with SessionLocal() as session:
            stats = get_dashboard_stats(session)

        self.assets_card.set_value(str(stats["assets"]))
        self.buildings_card.set_value(str(stats["buildings"]))
        self.contacts_card.set_value(str(stats["contacts"]))
        self.listings_card.set_value(str(stats["listings"]))
        self.events_card.set_value(str(stats["events"]))
        self.casafari_raw_card.set_value(str(stats["casafari_raw"]))
        self.casafari_resolved_card.set_value(str(stats["casafari_resolved"]))
        self.casafari_ambiguous_card.set_value(str(stats["casafari_ambiguous"]))
        self.casafari_unresolved_card.set_value(str(stats["casafari_unresolved"]))
        self.casafari_events_card.set_value(str(stats["casafari_events"]))