import os

import app.ui.widgets.leaflet_map_widget as leaflet_map_widget


def test_leaflet_widget_disables_embedded_map_in_safe_mode_on_windows(monkeypatch) -> None:
    if os.name != "nt":
        return

    monkeypatch.setattr(leaflet_map_widget, "WEB_ENGINE_AVAILABLE", True)
    monkeypatch.delenv("NEXUS_ENABLE_EMBEDDED_MAP", raising=False)
    monkeypatch.setenv("QT_QPA_PLATFORM", "")
    monkeypatch.setattr(leaflet_map_widget.QApplication, "instance", staticmethod(lambda: None))

    assert leaflet_map_widget._can_use_web_engine() is False
