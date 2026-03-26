from __future__ import annotations

from statistics import mean
from typing import Any

from sqlalchemy.orm import Session

from core.services.microzone_intelligence_service import get_microzone_intelligence
from core.services.opportunity_queue_service_v2 import (
    filter_opportunity_rows,
    get_opportunity_queue_v2,
)


MADRID_CENTER = {"lat": 40.4168, "lon": -3.7038}


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


def get_spatial_map_payload(
    session: Session,
    *,
    window_days: int = 14,
    event_type_filter: str = "all",
    min_score: float | None = None,
    zone_query: str | None = None,
    layer_mode: str = "both",
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

    return {
        "window_days": window_days,
        "event_type_filter": event_type_filter,
        "min_score": min_score,
        "zone_query": zone_query or "",
        "layer_mode": layer_mode,
        "summary": {
            "geo_opportunities_total": len(map_opportunities),
            "high_priority_geo_opportunities": _priority_count(map_opportunities, "alta"),
            "medium_priority_geo_opportunities": _priority_count(map_opportunities, "media"),
            "microzones_total": len(map_microzones),
            "microzone_hotspots": _microzone_count(map_microzones, 65.0),
            "avg_opportunity_score": round(
                mean(float(row.get("score") or 0.0) for row in map_opportunities), 1
            )
            if map_opportunities
            else 0.0,
        },
        "viewport": viewport,
        "points": map_opportunities,
        "microzones": map_microzones,
        "top_opportunities": map_opportunities[:12],
        "top_microzones": map_microzones[:12],
    }
