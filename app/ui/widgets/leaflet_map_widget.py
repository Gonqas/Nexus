from __future__ import annotations

import json
import os
from typing import Any

from PySide6.QtCore import QUrl
from PySide6.QtWidgets import QApplication, QLabel, QVBoxLayout, QWidget

try:
    from PySide6.QtWebEngineCore import QWebEngineSettings
    from PySide6.QtWebEngineWidgets import QWebEngineView

    WEB_ENGINE_AVAILABLE = True
except ImportError:
    QWebEngineSettings = None
    QWebEngineView = None
    WEB_ENGINE_AVAILABLE = False


def _can_use_web_engine() -> bool:
    if not WEB_ENGINE_AVAILABLE:
        return False

    if os.environ.get("QT_QPA_PLATFORM", "").lower() == "offscreen":
        return False

    app = QApplication.instance()
    if app and app.platformName().lower() == "offscreen":
        return False

    return True


def _leaflet_html(payload: dict[str, Any], selection: dict[str, Any] | None = None) -> str:
    payload_json = json.dumps(payload, ensure_ascii=False)
    selection_json = json.dumps(selection or {}, ensure_ascii=False)

    return f"""
<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <link
    rel="stylesheet"
    href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
  />
  <style>
    html, body, #map {{
      height: 100%;
      margin: 0;
      padding: 0;
      background: #f4f1eb;
      font-family: "Segoe UI", "Trebuchet MS", sans-serif;
    }}

    .leaflet-container {{
      background: #e9ece7;
    }}

    .map-summary {{
      background: rgba(255, 255, 255, 0.96);
      border: 1px solid #ddd4c7;
      border-radius: 14px;
      padding: 12px 14px;
      color: #1f2933;
      box-shadow: 0 8px 18px rgba(31, 41, 51, 0.08);
      min-width: 220px;
    }}

    .map-summary h4 {{
      margin: 0 0 8px 0;
      font-size: 14px;
      color: #7a5a44;
    }}

    .map-summary p {{
      margin: 0 0 4px 0;
      font-size: 12px;
    }}

    .leaflet-popup-content-wrapper,
    .leaflet-popup-tip {{
      background: #fffdf9;
      color: #1f2933;
    }}

    .popup-title {{
      font-weight: 700;
      margin-bottom: 6px;
      color: #1f2933;
    }}

    .popup-line {{
      margin: 2px 0;
      font-size: 12px;
      color: #51606d;
    }}
  </style>
</head>
<body>
  <div id="map"></div>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script>
    const payload = {payload_json};
    const selection = {selection_json};
    const map = L.map("map", {{
      zoomControl: true,
      preferCanvas: true,
    }});

    L.tileLayer("https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png", {{
      maxZoom: 19,
      attribution: "&copy; OpenStreetMap contributors",
    }}).addTo(map);

    const bounds = [];

    function escapeHtml(value) {{
      return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
    }}

    function priorityColor(priority) {{
      if (priority === "alta") return "#c14f2c";
      if (priority === "media") return "#dd8a4b";
      return "#71839a";
    }}

    function selectedStroke(isSelected) {{
      return isSelected ? "#223036" : "#ffffff";
    }}

    const opportunityLayer = L.layerGroup();
    payload.points.forEach((row) => {{
      const isSelected = selection.selected_event_id === row.event_id;
      const marker = L.circleMarker([row.lat, row.lon], {{
        radius: isSelected ? 10 : 7,
        color: selectedStroke(isSelected),
        weight: isSelected ? 3 : 2,
        fillColor: priorityColor(row.priority_label),
        fillOpacity: 0.92,
      }});

      marker.bindPopup(`
        <div class="popup-title">${{escapeHtml(row.asset_address || "Oportunidad")}}</div>
        <div class="popup-line">Score: ${{escapeHtml(row.score)}} | prioridad: ${{escapeHtml(row.priority_label)}}</div>
        <div class="popup-line">Evento: ${{escapeHtml(row.event_type)}} | portal: ${{escapeHtml(row.portal)}}</div>
        <div class="popup-line">Zona: ${{escapeHtml(row.zone_label)}}${{row.microzone_label ? " | " + escapeHtml(row.microzone_label) : ""}}</div>
        <div class="popup-line">Accion: ${{escapeHtml(row.zone_recommended_action)}}</div>
        <div class="popup-line">${{escapeHtml(row.reason)}}</div>
      `);

      if (isSelected) {{
        marker.openPopup();
      }}

      marker.addTo(opportunityLayer);
      bounds.push([row.lat, row.lon]);
    }});

    const microzoneLayer = L.layerGroup();
    payload.microzones.forEach((row) => {{
      const isSelected = selection.selected_microzone_label === row.microzone_label;
      const radius = Math.max(8, Math.min(18, 7 + (Number(row.microzone_capture_score || 0) / 12)));
      const marker = L.circleMarker([row.lat, row.lon], {{
        radius,
        color: isSelected ? "#223036" : "#a46140",
        weight: isSelected ? 3 : 2,
        fillColor: "#f3c78f",
        fillOpacity: isSelected ? 0.55 : 0.32,
      }});

      marker.bindPopup(`
        <div class="popup-title">${{escapeHtml(row.microzone_label)}}</div>
        <div class="popup-line">Base: ${{escapeHtml(row.parent_zone_label)}}</div>
        <div class="popup-line">Capture: ${{escapeHtml(row.microzone_capture_score)}} | confianza: ${{escapeHtml(row.microzone_confidence_score)}}</div>
        <div class="popup-line">Concentracion: ${{escapeHtml(row.microzone_concentration_score)}} | eventos 14d: ${{escapeHtml(row.events_14d)}}</div>
        <div class="popup-line">Accion: ${{escapeHtml(row.recommended_action)}}</div>
        <div class="popup-line">${{escapeHtml(row.radar_explanation)}}</div>
      `);

      if (isSelected) {{
        marker.openPopup();
      }}

      marker.addTo(microzoneLayer);
      bounds.push([row.lat, row.lon]);
    }});

    const overlays = {{}};
    if (payload.points.length) {{
      overlays["Oportunidades"] = opportunityLayer;
      opportunityLayer.addTo(map);
    }}
    if (payload.microzones.length) {{
      overlays["Microzonas"] = microzoneLayer;
      microzoneLayer.addTo(map);
    }}
    if (Object.keys(overlays).length > 1) {{
      L.control.layers(null, overlays, {{ collapsed: false }}).addTo(map);
    }}

    const viewport = payload.viewport || {{}};
    if (bounds.length) {{
      map.fitBounds(bounds, {{ padding: [30, 30] }});
    }} else {{
      const center = viewport.center || {{ lat: 40.4168, lon: -3.7038 }};
      map.setView([center.lat, center.lon], 12);
    }}

    const summary = payload.summary || {{}};
    const summaryControl = L.control({{ position: "topright" }});
    summaryControl.onAdd = function () {{
      const div = L.DomUtil.create("div", "map-summary");
      div.innerHTML = `
        <h4>Lectura espacial</h4>
        <p>Geo oportunidades: <strong>${{escapeHtml(summary.geo_opportunities_total || 0)}}</strong></p>
        <p>Alta prioridad: <strong>${{escapeHtml(summary.high_priority_geo_opportunities || 0)}}</strong></p>
        <p>Microzonas: <strong>${{escapeHtml(summary.microzones_total || 0)}}</strong></p>
        <p>Hotspots micro: <strong>${{escapeHtml(summary.microzone_hotspots || 0)}}</strong></p>
      `;
      return div;
    }};
    summaryControl.addTo(map);
  </script>
</body>
</html>
"""


class LeafletMapWidget(QWidget):
    def __init__(self) -> None:
        super().__init__()

        self.setMinimumHeight(620)
        self._payload: dict[str, Any] | None = None

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self._use_web_engine = _can_use_web_engine()
        if self._use_web_engine:
            self.web_view = QWebEngineView()
            if QWebEngineSettings is not None:
                settings = self.web_view.settings()
                settings.setAttribute(
                    QWebEngineSettings.WebAttribute.LocalContentCanAccessRemoteUrls,
                    True,
                )
            layout.addWidget(self.web_view)
            self.fallback_label = None
        else:
            self.web_view = None
            self.fallback_label = QLabel("Mapa no disponible en este modo de render. La vista sigue mostrando la lectura espacial en el panel lateral.")
            self.fallback_label.setWordWrap(True)
            self.fallback_label.setObjectName("PageSubtitle")
            layout.addWidget(self.fallback_label)

    def load_payload(
        self,
        payload: dict[str, Any],
        *,
        selection: dict[str, Any] | None = None,
    ) -> None:
        self._payload = payload

        if self.web_view is not None:
            self.web_view.setHtml(
                _leaflet_html(payload, selection=selection),
                QUrl("https://nexus-madrid.local/"),
            )
            return

        if self.fallback_label is not None:
            summary = payload.get("summary") or {}
            self.fallback_label.setText(
                "Modo sin mapa embebido.\n"
                f"Oportunidades geo: {summary.get('geo_opportunities_total', 0)} | "
                f"Microzonas: {summary.get('microzones_total', 0)} | "
                f"Alta prioridad: {summary.get('high_priority_geo_opportunities', 0)}"
            )
