import os
from pathlib import Path

from PySide6.QtWidgets import QApplication
import app.ui.widgets.leaflet_map_widget as leaflet_map_widget


def test_leaflet_widget_disables_embedded_map_in_safe_mode_on_windows(monkeypatch) -> None:
    if os.name != "nt":
        return

    monkeypatch.setattr(leaflet_map_widget, "WEB_ENGINE_AVAILABLE", True)
    monkeypatch.delenv("NEXUS_ENABLE_EMBEDDED_MAP", raising=False)
    monkeypatch.setenv("QT_QPA_PLATFORM", "")
    monkeypatch.setattr(leaflet_map_widget.QApplication, "instance", staticmethod(lambda: None))

    assert leaflet_map_widget._can_use_web_engine() is False


def test_leaflet_widget_exports_html_and_enables_browser_button(monkeypatch, tmp_path: Path) -> None:
    app = QApplication.instance() or QApplication([])
    monkeypatch.setattr(leaflet_map_widget, "MAP_EXPORT_PATH", tmp_path / "map_preview.html")
    monkeypatch.setattr(leaflet_map_widget, "_can_use_web_engine", lambda: False)

    widget = leaflet_map_widget.LeafletMapWidget()
    widget.load_payload(
        {
            "summary": {
                "geo_opportunities_total": 2,
                "microzones_total": 1,
                "high_priority_geo_opportunities": 1,
            },
            "points": [],
            "microzones": [],
        }
    )

    assert leaflet_map_widget.MAP_EXPORT_PATH.exists()
    assert widget.open_external_button.isEnabled() is True
