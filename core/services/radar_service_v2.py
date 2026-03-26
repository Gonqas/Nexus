from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from core.services.microzone_intelligence_service import get_microzone_intelligence
from core.services.zone_intelligence_service_v2 import get_zone_intelligence_v2


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]

    ordered = sorted(values)
    pos = (len(ordered) - 1) * q
    lower = int(pos)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = pos - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _clip_scores(rows: list[dict], key: str) -> dict[str, float]:
    values = [float(row.get(key) or 0.0) for row in rows]
    if len(values) < 6:
        return {row["zone_label"]: float(row.get(key) or 0.0) for row in rows}

    low = _percentile(values, 0.10)
    high = _percentile(values, 0.90)
    clipped: dict[str, float] = {}

    for row in rows:
        value = float(row.get(key) or 0.0)
        clipped[row["zone_label"]] = min(max(value, low), high)

    return clipped


def _enrich_rows(rows: list[dict]) -> list[dict]:
    capture_clipped = _clip_scores(rows, "zone_capture_score")
    heat_clipped = _clip_scores(rows, "zone_heat_score")
    pressure_clipped = _clip_scores(rows, "zone_pressure_score")
    liquidity_clipped = _clip_scores(rows, "zone_liquidity_score")

    enriched: list[dict] = []
    for row in rows:
        enriched_row = dict(row)
        zone_label = row["zone_label"]
        enriched_row["_capture_sort"] = capture_clipped.get(zone_label, 0.0)
        enriched_row["_heat_sort"] = heat_clipped.get(zone_label, 0.0)
        enriched_row["_pressure_sort"] = pressure_clipped.get(zone_label, 0.0)
        enriched_row["_liquidity_sort"] = liquidity_clipped.get(zone_label, 0.0)
        enriched_row["radar_explanation"] = row.get("executive_summary") or row.get("score_explanation")
        enriched.append(enriched_row)

    return enriched


def _summary(rows: list[dict], window_days: int) -> dict[str, Any]:
    return {
        "window_days": window_days,
        "zones_total": len(rows),
        "high_confidence_zones": sum(1 for row in rows if (row.get("zone_confidence_score") or 0) >= 60),
        "low_confidence_zones": sum(1 for row in rows if (row.get("zone_confidence_score") or 0) < 40),
        "capture_ready_zones": sum(
            1
            for row in rows
            if (row.get("zone_capture_score") or 0) >= 60 and (row.get("zone_confidence_score") or 0) >= 50
        ),
        "hot_zones": sum(1 for row in rows if (row.get("zone_heat_score") or 0) >= 65),
        "relative_hot_zones": sum(1 for row in rows if (row.get("zone_relative_heat_score") or 0) >= 65),
        "transform_zones": sum(1 for row in rows if (row.get("zone_transformation_signal_score") or 0) >= 65),
        "predictive_zones": sum(
            1 for row in rows if (row.get("predicted_absorption_30d_score") or 0) >= 65
        ),
    }


def get_radar_payload_v2(session: Session, window_days: int = 14) -> dict[str, Any]:
    rows = get_zone_intelligence_v2(session, window_days=window_days)
    rows = _enrich_rows(rows)
    microzones = get_microzone_intelligence(session, window_days=window_days)

    top_capture = sorted(
        rows,
        key=lambda r: (r["_capture_sort"], r["zone_confidence_score"], r["zone_heat_score"]),
        reverse=True,
    )[:12]

    top_heat = sorted(
        rows,
        key=lambda r: (
            r["_heat_sort"],
            r.get("zone_relative_heat_score") or 0.0,
            r.get("events_14d_per_10k_population") or 0.0,
            r["zone_confidence_score"],
        ),
        reverse=True,
    )[:12]

    top_pressure = sorted(
        rows,
        key=lambda r: (r["_pressure_sort"], r["price_drop_count"], r["zone_confidence_score"]),
        reverse=True,
    )[:12]

    top_transformation = sorted(
        rows,
        key=lambda r: (
            r.get("zone_transformation_signal_score") or 0.0,
            r.get("change_of_use_per_10k_population") or 0.0,
            r.get("closed_locales_per_1k_population") or 0.0,
            r["zone_confidence_score"],
        ),
        reverse=True,
    )[:12]

    top_liquidity = sorted(
        rows,
        key=lambda r: (r["_liquidity_sort"], r["absorption_count"], r["zone_confidence_score"]),
        reverse=True,
    )[:12]

    top_predictive = sorted(
        rows,
        key=lambda r: (
            r.get("predicted_absorption_30d_score") or 0.0,
            r.get("zone_liquidity_score") or 0.0,
            r.get("zone_relative_heat_score") or 0.0,
            r.get("zone_confidence_score") or 0.0,
        ),
        reverse=True,
    )[:12]

    low_confidence = sorted(
        rows,
        key=lambda r: (r["zone_confidence_score"], -r["casafari_raw_in_zone"]),
    )[:12]

    top_microzones = sorted(
        microzones,
        key=lambda row: (
            row.get("microzone_capture_score") or 0.0,
            row.get("microzone_concentration_score") or 0.0,
            row.get("microzone_confidence_score") or 0.0,
            row.get("events_14d") or 0,
        ),
        reverse=True,
    )[:12]

    summary = _summary(rows, window_days=window_days)
    summary["microzones_total"] = len(microzones)
    summary["microzone_hotspots"] = sum(
        1 for row in microzones if (row.get("microzone_capture_score") or 0) >= 65
    )

    return {
        "window_days": window_days,
        "summary": summary,
        "top_capture": top_capture,
        "top_heat": top_heat,
        "top_pressure": top_pressure,
        "top_transformation": top_transformation,
        "top_liquidity": top_liquidity,
        "top_predictive": top_predictive,
        "low_confidence": low_confidence,
        "top_microzones": top_microzones,
    }
