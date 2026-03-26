from core.services.predictive_signal_service import (
    build_opportunity_prediction,
    build_zone_prediction,
)


def test_zone_prediction_rewards_liquidity_relative_heat_and_confidence() -> None:
    prediction = build_zone_prediction(
        {
            "zone_liquidity_score": 72.0,
            "zone_heat_score": 66.0,
            "zone_relative_heat_score": 70.0,
            "zone_confidence_score": 64.0,
            "zone_transformation_signal_score": 48.0,
            "zone_pressure_score": 54.0,
            "zone_capture_score": 68.0,
            "zone_saturation_score": 42.0,
        }
    )

    assert prediction["predicted_absorption_30d_score"] > 60
    assert prediction["predicted_absorption_30d_band"] in {"alta", "media"}
    assert prediction["predicted_action_window_days"] in {7, 14}


def test_opportunity_prediction_uses_zone_micro_and_event_context() -> None:
    prediction = build_opportunity_prediction(
        zone_row={
            "zone_liquidity_score": 70.0,
            "zone_heat_score": 65.0,
            "zone_relative_heat_score": 72.0,
            "zone_confidence_score": 60.0,
            "zone_transformation_signal_score": 44.0,
            "zone_pressure_score": 52.0,
            "zone_capture_score": 67.0,
            "zone_saturation_score": 40.0,
        },
        microzone_row={
            "microzone_capture_score": 74.0,
            "microzone_concentration_score": 68.0,
            "microzone_confidence_score": 58.0,
        },
        event_type="price_drop",
        price_drop_pct=0.12,
        has_geo_point=True,
    )

    assert prediction["predicted_opportunity_30d_score"] > 60
    assert prediction["predicted_opportunity_30d_band"] in {"alta", "media"}
    assert "zona" in prediction["prediction_explanation"]
