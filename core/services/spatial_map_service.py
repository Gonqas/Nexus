from __future__ import annotations

from statistics import mean
from typing import Any

from sqlalchemy.orm import Session

from core.services.microzone_intelligence_service import get_microzone_intelligence
from core.services.opportunity_queue_service_v2 import (
    filter_opportunity_rows,
    get_opportunity_queue_v2,
)
from core.services.spatial_boundary_service import (
    get_boundary_layer_meta,
    load_official_boundary_topology,
)
from core.services.zone_intelligence_service_v2 import get_zone_intelligence_v2
from core.normalization.text import normalize_text_key


MADRID_CENTER = {"lat": 40.4168, "lon": -3.7038}
ZONE_METRIC_DEFS = {
    "capture": ("zone_capture_score", "Capture"),
    "heat": ("zone_heat_score", "Heat"),
    "relative_heat": ("zone_relative_heat_score", "Heat relativo"),
    "pressure": ("zone_pressure_score", "Pressure"),
    "transformation": ("zone_transformation_signal_score", "Transformacion"),
    "predictive": ("predicted_absorption_30d_score", "Prediccion 30d"),
    "confidence": ("zone_confidence_score", "Confianza"),
    "liquidity": ("zone_liquidity_score", "Liquidez"),
}


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _compute_viewport(points: list[dict], microzones: list[dict]) -> dict[str, Any]:
    latitudes: list[float] = []
    longitudes: list[float] = []

    for row in points:
        lat = _safe_float(row.get("lat"))
        lon = _safe_float(row.get("lon"))
        if lat is None or lon is None:
            continue
        latitudes.append(lat)
        longitudes.append(lon)

    for row in microzones:
        lat = _safe_float(row.get("lat"))
        lon = _safe_float(row.get("lon"))
        if lat is None or lon is None:
            continue
        latitudes.append(lat)
        longitudes.append(lon)

    if not latitudes or not longitudes:
        return {
            "center": dict(MADRID_CENTER),
            "bounds": None,
        }

    return {
        "center": {
            "lat": round(mean(latitudes), 6),
            "lon": round(mean(longitudes), 6),
        },
        "bounds": {
            "south": round(min(latitudes), 6),
            "west": round(min(longitudes), 6),
            "north": round(max(latitudes), 6),
            "east": round(max(longitudes), 6),
        },
    }


def _priority_count(rows: list[dict], label: str) -> int:
    return sum(1 for row in rows if (row.get("priority_label") or "") == label)


def _microzone_count(rows: list[dict], threshold: float) -> int:
    return sum(
        1 for row in rows if float(row.get("microzone_capture_score") or 0.0) >= threshold
    )


def _build_opportunity_points(rows: list[dict], limit: int) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    for row in rows[:limit]:
        lat = _safe_float(row.get("asset_lat"))
        lon = _safe_float(row.get("asset_lon"))
        if lat is None or lon is None:
            continue

        points.append(
            {
                "event_id": row.get("event_id"),
                "lat": lat,
                "lon": lon,
                "score": row.get("score"),
                "priority_label": row.get("priority_label"),
                "event_type": row.get("event_type"),
                "zone_label": row.get("zone_label"),
                "microzone_label": row.get("microzone_label"),
                "portal": row.get("portal"),
                "asset_address": row.get("asset_address"),
                "reason": row.get("reason"),
                "zone_recommended_action": row.get("zone_recommended_action"),
                "predicted_opportunity_30d_band": row.get("predicted_opportunity_30d_band"),
                "predicted_opportunity_30d_score": row.get("predicted_opportunity_30d_score"),
            }
        )
    return points


def _filter_microzones(rows: list[dict], *, min_score: float | None, zone_query: str | None) -> list[dict]:
    query = (zone_query or "").strip().lower()
    filtered: list[dict] = []

    for row in rows:
        if row.get("centroid_lat") is None or row.get("centroid_lon") is None:
            continue

        if min_score is not None and float(row.get("microzone_capture_score") or 0.0) < min_score:
            continue

        if query:
            haystack = " ".join(
                str(value).lower()
                for value in (
                    row.get("microzone_label"),
                    row.get("parent_zone_label"),
                    row.get("recommended_action"),
                )
                if value
            )
            if query not in haystack:
                continue

        filtered.append(row)

    return filtered


def _build_microzone_points(rows: list[dict], limit: int) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    for row in rows[:limit]:
        points.append(
            {
                "microzone_label": row.get("microzone_label"),
                "parent_zone_label": row.get("parent_zone_label"),
                "lat": float(row["centroid_lat"]),
                "lon": float(row["centroid_lon"]),
                "microzone_capture_score": row.get("microzone_capture_score"),
                "microzone_concentration_score": row.get("microzone_concentration_score"),
                "microzone_confidence_score": row.get("microzone_confidence_score"),
                "events_14d": row.get("events_14d"),
                "recommended_action": row.get("recommended_action"),
                "radar_explanation": row.get("radar_explanation"),
            }
        )
    return points


def _boundary_zone_lookup(rows: list[dict]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for row in rows:
        zone_label = row.get("zone_label")
        zone_key = normalize_text_key(zone_label)
        if not zone_key:
            continue
        lookup[zone_key] = row
    return lookup


def _build_boundary_layers(
    zone_rows: list[dict],
    *,
    metric_mode: str,
    min_score: float | None = None,
    zone_query: str | None = None,
) -> dict[str, Any]:
    metric_key, metric_label = ZONE_METRIC_DEFS.get(metric_mode, ZONE_METRIC_DEFS["capture"])
    query_key = normalize_text_key(zone_query)
    filtered_zone_rows: list[dict] = []
    for row in zone_rows:
        metric_value = float(row.get(metric_key) or 0.0)
        if min_score is not None and metric_value < min_score:
            continue
        if query_key:
            zone_key = normalize_text_key(row.get("zone_label"))
            summary_key = normalize_text_key(row.get("executive_summary") or row.get("score_explanation"))
            if query_key not in " ".join(part for part in (zone_key, summary_key) if part):
                continue
        filtered_zone_rows.append(row)

    zone_lookup = _boundary_zone_lookup(filtered_zone_rows)

    layers: dict[str, Any] = {}
    for level in ("districts", "neighborhoods"):
        topology = load_official_boundary_topology(level)
        if not topology:
            continue

        meta = get_boundary_layer_meta(level)
        object_name = meta["object_name"]
        geometries = (((topology.get("objects") or {}).get(object_name) or {}).get("geometries") or [])

        metric_lookup: dict[str, Any] = {}
        matched_rows: list[dict[str, Any]] = []
        for geometry in geometries:
            properties = geometry.get("properties") or {}
            zone_label = properties.get(meta["label_key"])
            zone_key = normalize_text_key(zone_label)
            if not zone_key:
                continue
            row = zone_lookup.get(zone_key)
            if not row:
                continue

            metric_lookup[zone_key] = {
                "zone_label": row.get("zone_label"),
                "metric_value": row.get(metric_key),
                "recommended_action": row.get("recommended_action"),
                "score_explanation": row.get("score_explanation"),
                "zone_confidence_score": row.get("zone_confidence_score"),
                "zone_relative_heat_score": row.get("zone_relative_heat_score"),
                "zone_capture_score": row.get("zone_capture_score"),
                "zone_transformation_signal_score": row.get(
                    "zone_transformation_signal_score"
                ),
                "zone_pressure_score": row.get("zone_pressure_score"),
                "zone_liquidity_score": row.get("zone_liquidity_score"),
                "predicted_absorption_30d_score": row.get(
                    "predicted_absorption_30d_score"
                ),
                "official_population": row.get("official_population"),
                "events_14d_per_10k_population": row.get(
                    "events_14d_per_10k_population"
                ),
            }
            matched_rows.append(row)

        layers[level] = {
            "topology": topology,
            "object_name": object_name,
            "label_key": meta["label_key"],
            "parent_key": meta["parent_key"],
            "metric_key": metric_key,
            "metric_label": metric_label,
            "matched_count": len(metric_lookup),
            "zone_lookup": metric_lookup,
        }

    sorted_rows = sorted(
        filtered_zone_rows,
        key=lambda row: (
            float(row.get(metric_key) or 0.0),
            float(row.get("zone_confidence_score") or 0.0),
            float(row.get("zone_relative_heat_score") or 0.0),
        ),
        reverse=True,
    )

    return {
        "metric_mode": metric_mode,
        "metric_key": metric_key,
        "metric_label": metric_label,
        "layers": layers,
        "top_zones": sorted_rows[:12],
    }


def get_spatial_map_payload(
    session: Session,
    *,
    window_days: int = 14,
    event_type_filter: str = "all",
    min_score: float | None = None,
    zone_query: str | None = None,
    layer_mode: str = "both",
    boundary_level: str = "neighborhoods",
    zone_metric_mode: str = "capture",
    heat_mode: str = "on",
    opportunity_limit: int = 250,
    microzone_limit: int = 160,
) -> dict[str, Any]:
    queue_rows = get_opportunity_queue_v2(
        session,
        window_days=window_days,
        limit=max(opportunity_limit * 2, 400),
    )
    filtered_opportunities = filter_opportunity_rows(
        queue_rows,
        event_type_filter=event_type_filter,
        geo_filter="with_geo",
        min_score=min_score,
        zone_query=zone_query,
    )
    opportunity_points = _build_opportunity_points(filtered_opportunities, opportunity_limit)

    microzones = get_microzone_intelligence(session, window_days=window_days, limit=None)
    filtered_microzones = _filter_microzones(
        microzones,
        min_score=min_score,
        zone_query=zone_query,
    )
    filtered_microzones.sort(
        key=lambda row: (
            float(row.get("microzone_capture_score") or 0.0),
            float(row.get("microzone_concentration_score") or 0.0),
            float(row.get("microzone_confidence_score") or 0.0),
            int(row.get("events_14d") or 0),
        ),
        reverse=True,
    )
    microzone_points = _build_microzone_points(filtered_microzones, microzone_limit)
    zone_rows = get_zone_intelligence_v2(session, window_days=window_days)
    boundary_payload = _build_boundary_layers(
        zone_rows,
        metric_mode=zone_metric_mode,
        min_score=min_score,
        zone_query=zone_query,
    )

    if layer_mode == "opportunities":
        map_opportunities = opportunity_points
        map_microzones: list[dict[str, Any]] = []
    elif layer_mode == "microzones":
        map_opportunities = []
        map_microzones = microzone_points
    else:
        map_opportunities = opportunity_points
        map_microzones = microzone_points

    viewport = _compute_viewport(map_opportunities, map_microzones)
    heat_points = [
        [row["lat"], row["lon"], round(min(max(float(row.get("score") or 0.0) / 80.0, 0.2), 1.0), 3)]
        for row in map_opportunities
    ]

    return {
        "window_days": window_days,
        "event_type_filter": event_type_filter,
        "min_score": min_score,
        "zone_query": zone_query or "",
        "layer_mode": layer_mode,
        "boundary_level": boundary_level,
        "zone_metric": {
            "mode": boundary_payload["metric_mode"],
            "key": boundary_payload["metric_key"],
            "label": boundary_payload["metric_label"],
        },
        "heat_mode": heat_mode,
        "summary": {
            "geo_opportunities_total": len(map_opportunities),
            "high_priority_geo_opportunities": _priority_count(map_opportunities, "alta"),
            "medium_priority_geo_opportunities": _priority_count(map_opportunities, "media"),
            "microzones_total": len(map_microzones),
            "microzone_hotspots": _microzone_count(map_microzones, 65.0),
            "zones_with_boundaries": int(
                (
                    (
                        boundary_payload["layers"].get(boundary_level, {})
                        if boundary_level in {"districts", "neighborhoods"}
                        else {}
                    ).get("matched_count")
                    or 0
                )
            ),
            "avg_opportunity_score": round(
                mean(float(row.get("score") or 0.0) for row in map_opportunities), 1
            )
            if map_opportunities
            else 0.0,
        },
        "viewport": viewport,
        "points": map_opportunities,
        "microzones": map_microzones,
        "heat_points": heat_points if heat_mode == "on" else [],
        "boundaries": boundary_payload["layers"],
        "top_opportunities": map_opportunities[:12],
        "top_microzones": map_microzones[:12],
        "top_zones": boundary_payload["top_zones"],
    }
