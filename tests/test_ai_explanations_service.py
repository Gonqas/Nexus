from core.services.ai_explanations_service import (
    explain_casafari_case,
    explain_opportunity_row,
    explain_zone_row,
)


def test_explain_zone_row_produces_clear_summary_and_next_step() -> None:
    row = {
        "zone_label": "Prosperidad",
        "zone_capture_score": 68.0,
        "zone_heat_score": 64.0,
        "zone_relative_heat_score": 71.0,
        "zone_transformation_signal_score": 52.0,
        "zone_liquidity_score": 60.0,
        "zone_confidence_score": 74.0,
        "zone_pressure_score": 49.0,
        "predicted_absorption_30d_score": 63.0,
        "predicted_action_window_days": 14,
        "events_14d_per_10k_population": 0.7,
        "official_population": 36961,
        "official_vulnerability_index": 6.1,
        "recommended_action": "Seguir y vigilar",
    }

    explanation = explain_zone_row(row)

    assert "Prosperidad" in explanation["ai_summary"]
    assert explanation["ai_context_line"]
    assert "14" in explanation["ai_next_step"] or "2 semanas" in explanation["ai_next_step"]


def test_explain_opportunity_row_highlights_drivers() -> None:
    row = {
        "event_type": "listing_detected",
        "zone_label": "Guindalera",
        "priority_label": "alta",
        "score_zone_signal": 8.5,
        "score_microzone_signal": 5.2,
        "score_geo_signal": 12.0,
        "score_price_signal": 0.0,
        "predicted_opportunity_30d_score": 58.0,
        "predicted_opportunity_30d_band": "media",
        "predicted_action_window_days": 14,
        "zone_confidence_score": 72.0,
        "has_geo_point": True,
        "score_event_base": 32.0,
        "score_recency": 14.0,
        "score_predictive_signal": 7.0,
    }

    explanation = explain_opportunity_row(row)

    assert "Guindalera" in explanation["ai_summary"]
    assert "Prioridad alta" in explanation["ai_summary"]
    assert "14" in explanation["ai_next_step"]
    assert explanation["ai_score_story"]


def test_explain_casafari_case_translates_taxonomy_to_human_text() -> None:
    row = {
        "match_status": "unresolved",
        "reason_taxonomy": "weak_identity",
        "address_precision": "unknown",
        "match_confidence_band": "low",
        "phone_profile": "broker_like",
        "price_confidence": "low",
    }

    explanation = explain_casafari_case(row)

    assert "Caso sin resolver" in explanation["ai_summary"]
    assert "identidad" in explanation["ai_summary"].lower() or "senales" in explanation["ai_summary"].lower()
    assert "No enlazar aun" in explanation["ai_next_step"]
