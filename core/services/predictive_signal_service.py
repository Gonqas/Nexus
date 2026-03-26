from __future__ import annotations


def _clamp_0_100(value: float) -> float:
    return min(max(value, 0.0), 100.0)


def prediction_band(score: float) -> str:
    if score >= 72:
        return "alta"
    if score >= 56:
        return "media"
    if score >= 40:
        return "moderada"
    return "baja"


def recommended_action_window_days(score: float) -> int:
    if score >= 72:
        return 7
    if score >= 56:
        return 14
    return 30


def build_zone_prediction(row: dict) -> dict:
    liquidity = float(row.get("zone_liquidity_score") or 0.0)
    heat = float(row.get("zone_heat_score") or 0.0)
    relative_heat = float(row.get("zone_relative_heat_score") or 0.0)
    confidence = float(row.get("zone_confidence_score") or 0.0)
    transformation = float(row.get("zone_transformation_signal_score") or 0.0)
    pressure = float(row.get("zone_pressure_score") or 0.0)
    capture = float(row.get("zone_capture_score") or 0.0)
    saturation = float(row.get("zone_saturation_score") or 0.0)

    pressure_penalty = max(pressure - liquidity, 0.0) * 0.08
    saturation_penalty = max(saturation - 58.0, 0.0) * 0.05

    score = (
        0.26 * liquidity
        + 0.18 * heat
        + 0.18 * relative_heat
        + 0.14 * confidence
        + 0.12 * capture
        + 0.08 * transformation
        + 0.06 * pressure
        - pressure_penalty
        - saturation_penalty
    )
    score = round(_clamp_0_100(score), 1)

    band = prediction_band(score)
    window_days = recommended_action_window_days(score)
    explanation_bits = [
        f"liquidez {liquidity:.1f}",
        f"heat {heat:.1f}",
        f"relative {relative_heat:.1f}",
        f"confidence {confidence:.1f}",
    ]
    if transformation >= 45:
        explanation_bits.append(f"transformacion {transformation:.1f}")
    if pressure_penalty > 0:
        explanation_bits.append("penaliza presion>sliquidez")
    if saturation_penalty > 0:
        explanation_bits.append("penaliza saturacion")

    return {
        "predicted_absorption_30d_score": score,
        "predicted_absorption_30d_band": band,
        "predicted_action_window_days": window_days,
        "prediction_explanation": " | ".join(explanation_bits),
    }


def build_opportunity_prediction(
    *,
    zone_row: dict | None,
    microzone_row: dict | None,
    event_type: str | None,
    price_drop_pct: float | None,
    has_geo_point: bool,
) -> dict:
    zone_prediction = build_zone_prediction(zone_row or {})
    zone_score = float(zone_prediction["predicted_absorption_30d_score"])
    micro_capture = float((microzone_row or {}).get("microzone_capture_score") or 0.0)
    micro_concentration = float((microzone_row or {}).get("microzone_concentration_score") or 0.0)
    micro_confidence = float((microzone_row or {}).get("microzone_confidence_score") or 0.0)

    event_bonus = 0.0
    if event_type == "listing_detected":
        event_bonus = 12.0
    elif event_type == "price_drop":
        event_bonus = 14.0
    elif event_type in {"reserved", "sold"}:
        event_bonus = 8.0
    elif event_type in {"expired", "not_available"}:
        event_bonus = 4.0

    drop_bonus = 0.0
    if price_drop_pct is not None:
        if price_drop_pct >= 0.15:
            drop_bonus = 10.0
        elif price_drop_pct >= 0.08:
            drop_bonus = 6.0
        elif price_drop_pct >= 0.03:
            drop_bonus = 3.0

    geo_bonus = 4.0 if has_geo_point else 0.0
    micro_bonus = (
        0.45 * micro_capture
        + 0.35 * micro_concentration
        + 0.20 * micro_confidence
    ) / 10.0

    score = round(
        _clamp_0_100(
            0.58 * zone_score
            + 0.22 * micro_bonus
            + event_bonus
            + drop_bonus
            + geo_bonus
        ),
        1,
    )

    band = prediction_band(score)
    window_days = recommended_action_window_days(score)
    explanation_bits = [
        f"zona {zone_score:.1f}",
        f"micro {micro_bonus:.1f}",
        f"evento {event_bonus:.1f}",
    ]
    if drop_bonus > 0:
        explanation_bits.append(f"bajada {drop_bonus:.1f}")
    if geo_bonus > 0:
        explanation_bits.append("geo precisa")

    return {
        "predicted_opportunity_30d_score": score,
        "predicted_opportunity_30d_band": band,
        "predicted_action_window_days": window_days,
        "prediction_explanation": " | ".join(explanation_bits),
        "zone_prediction": zone_prediction,
    }
