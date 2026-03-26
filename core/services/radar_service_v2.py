from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

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
    }


def get_radar_payload_v2(session: Session, window_days: int = 14) -> dict[str, Any]:
    rows = get_zone_intelligence_v2(session, window_days=window_days)
    rows = _enrich_rows(rows)

    top_capture = sorted(
        rows,
        key=lambda r: (r["_capture_sort"], r["zone_confidence_score"], r["zone_heat_score"]),
        reverse=True,
    )[:12]

    top_heat = sorted(
        rows,
        key=lambda r: (r["_heat_sort"], r["events_14d"], r["zone_confidence_score"]),
        reverse=True,
    )[:12]

    top_pressure = sorted(
        rows,
        key=lambda r: (r["_pressure_sort"], r["price_drop_count"], r["zone_confidence_score"]),
        reverse=True,
    )[:12]

    top_liquidity = sorted(
        rows,
        key=lambda r: (r["_liquidity_sort"], r["absorption_count"], r["zone_confidence_score"]),
        reverse=True,
    )[:12]

    low_confidence = sorted(
        rows,
        key=lambda r: (r["zone_confidence_score"], -r["casafari_raw_in_zone"]),
    )[:12]

    return {
        "window_days": window_days,
        "summary": _summary(rows, window_days=window_days),
        "top_capture": top_capture,
        "top_heat": top_heat,
        "top_pressure": top_pressure,
        "top_liquidity": top_liquidity,
        "low_confidence": low_confidence,
    }
