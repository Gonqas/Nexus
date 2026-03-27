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

    if os.name == "nt" and os.environ.get("NEXUS_ENABLE_EMBEDDED_MAP", "0") != "1":
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
  <script src="https://unpkg.com/topojson-client@3"></script>
  <script src="https://unpkg.com/leaflet.heat/dist/leaflet-heat.js"></script>
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

    function normalizeKey(value) {{
      return String(value || "")
        .normalize("NFD")
        .replace(/[\\u0300-\\u036f]/g, "")
        .toLowerCase()
        .replace(/[^a-z0-9\\s,.-]/g, "")
        .replace(/\\s+/g, " ")
        .trim();
    }}

    function zoneColor(value) {{
      const score = Number(value || 0);
      if (score >= 75) return "#a33823";
      if (score >= 62) return "#c45c33";
      if (score >= 50) return "#dd8a4b";
      if (score >= 38) return "#f0b36d";
      if (score > 0) return "#f6d8a8";
      return "#ebe5dc";
    }}

    function zoneStroke(isSelected) {{
      return isSelected ? "#1f2933" : "#a89682";
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

    const boundaryOverlays = {{}};
    const activeBoundaryLevel = payload.boundary_level || "neighborhoods";
    const zoneMetric = payload.zone_metric || {{}};
    const boundaries = payload.boundaries || {{}};

    function buildBoundaryLayer(level, title) {{
      const layerPayload = boundaries[level];
      if (!layerPayload || !layerPayload.topology) {{
        return null;
      }}

      const topology = layerPayload.topology;
      const objectName = layerPayload.object_name;
      const objectDef = topology.objects[objectName];
      if (!objectDef) {{
        return null;
      }}

      const features = topojson.feature(topology, objectDef).features;
      const zoneLookup = layerPayload.zone_lookup || {{}};
      const labelKey = layerPayload.label_key;
      const parentKey = layerPayload.parent_key;

      return L.geoJSON(features, {{
        style: function(feature) {{
          const props = feature.properties || {{}};
          const zoneKey = normalizeKey(props[labelKey]);
          const zoneData = zoneLookup[zoneKey] || {{}};
          const isSelected = selection.selected_zone_label && normalizeKey(selection.selected_zone_label) === zoneKey;
          return {{
            color: zoneStroke(isSelected),
            weight: isSelected ? 3 : 1.2,
            fillColor: zoneColor(zoneData.metric_value),
            fillOpacity: activeBoundaryLevel === level ? 0.34 : 0.18,
          }};
        }},
        onEachFeature: function(feature, layer) {{
          const props = feature.properties || {{}};
          const zoneKey = normalizeKey(props[labelKey]);
          const zoneData = zoneLookup[zoneKey] || {{}};
          const zoneLabel = props[labelKey] || "Zona";
          const parentLabel = parentKey ? props[parentKey] : null;
          const parentLine = parentLabel
            ? `<div class="popup-line">Distrito: ${{escapeHtml(parentLabel)}}</div>`
            : "";

          layer.bindPopup(`
            <div class="popup-title">${{escapeHtml(zoneLabel)}}</div>
            <div class="popup-line">Nivel: ${{escapeHtml(title)}}</div>
            ${{parentLine}}
            <div class="popup-line">${{escapeHtml(zoneMetric.label || "Metrica")}}: ${{escapeHtml(zoneData.metric_value ?? "-")}}</div>
            <div class="popup-line">Confianza: ${{escapeHtml(zoneData.zone_confidence_score ?? "-")}} | Heat rel: ${{escapeHtml(zoneData.zone_relative_heat_score ?? "-")}}</div>
            <div class="popup-line">Accion: ${{escapeHtml(zoneData.recommended_action ?? "-")}}</div>
            <div class="popup-line">${{escapeHtml(zoneData.score_explanation ?? "")}}</div>
          `);
        }},
      }});
    }}

    const districtLayer = buildBoundaryLayer("districts", "Distritos");
    const neighborhoodLayer = buildBoundaryLayer("neighborhoods", "Barrios");
    if (districtLayer) {{
      boundaryOverlays["Distritos"] = districtLayer;
      if (activeBoundaryLevel === "districts") {{
        districtLayer.addTo(map);
      }}
    }}
    if (neighborhoodLayer) {{
      boundaryOverlays["Barrios"] = neighborhoodLayer;
      if (activeBoundaryLevel === "neighborhoods") {{
        neighborhoodLayer.addTo(map);
      }}
    }}

    let heatLayer = null;
    if ((payload.heat_points || []).length) {{
      heatLayer = L.heatLayer(payload.heat_points, {{
        radius: 26,
        blur: 22,
        maxZoom: 16,
        minOpacity: 0.22,
        gradient: {{
          0.2: "#f6d8a8",
          0.45: "#f0b36d",
          0.65: "#dd8a4b",
          0.85: "#c45c33",
          1.0: "#8e311e",
        }},
      }});
      if ((payload.heat_mode || "off") === "on") {{
        heatLayer.addTo(map);
      }}
    }}

    const overlays = {{}};
    if (payload.points.length) {{
      overlays["Oportunidades"] = opportunityLayer;
      opportunityLayer.addTo(map);
    }}
    if (payload.microzones.length) {{
      overlays["Microzonas"] = microzoneLayer;
      microzoneLayer.addTo(map);
    }}
    Object.assign(overlays, boundaryOverlays);
    if (heatLayer) {{
      overlays["Heat"] = heatLayer;
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
        <p>Superficie: <strong>${{escapeHtml((payload.boundary_level || "none"))}}</strong></p>
        <p>Metrica zona: <strong>${{escapeHtml((zoneMetric.label || "-"))}}</strong></p>
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
        self._web_engine_failed = False

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
            self.web_view.renderProcessTerminated.connect(self._handle_web_engine_termination)
            layout.addWidget(self.web_view)
            self.fallback_label = None
        else:
            self.web_view = None
            self.fallback_label = QLabel(
                "Mapa embebido desactivado en modo seguro. La lectura espacial sigue disponible en el panel lateral."
            )
            self.fallback_label.setWordWrap(True)
            self.fallback_label.setObjectName("PageSubtitle")
            layout.addWidget(self.fallback_label)

    def _handle_web_engine_termination(self, *_args) -> None:
        self._web_engine_failed = True
        if self.web_view is not None:
            self.web_view.hide()
        if self.fallback_label is None:
            self.fallback_label = QLabel("")
            self.fallback_label.setWordWrap(True)
            self.fallback_label.setObjectName("PageSubtitle")
            self.layout().addWidget(self.fallback_label)
        self.fallback_label.show()
        self.fallback_label.setText(
            "El motor del mapa embebido ha fallado y la app ha pasado a modo seguro. "
            "Puedes seguir usando la lectura espacial desde el panel lateral sin perder el trabajo."
        )

    def load_payload(
        self,
        payload: dict[str, Any],
        *,
        selection: dict[str, Any] | None = None,
    ) -> None:
        self._payload = payload

        if self.web_view is not None and not self._web_engine_failed:
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
