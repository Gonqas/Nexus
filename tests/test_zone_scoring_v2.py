from __future__ import annotations

from core.scoring.zone_scoring_v2 import score_zone_rows_v2


def _base_row(zone_label: str, *, events_per_pop: float, listings_per_pop: float) -> dict:
    return {
        "zone_label": zone_label,
        "csv_freshness_days": 2,
        "assets_count": 10,
        "active_listings_count": 5,
        "broker_phone_share": 0.4,
        "listings_per_asset": 1.5,
        "events_14d": 4,
        "listing_detected_count": 2,
        "price_drop_count": 1,
        "absorption_count": 2,
        "resolved_ratio": 0.7,
        "asset_type_diversity": 2,
        "portal_diversity": 2,
        "geo_point_assets": 8,
        "geo_neighborhood_assets": 9,
        "official_population": 10000,
        "events_14d_per_10k_population": events_per_pop,
        "active_listings_per_1k_population": listings_per_pop,
        "price_drop_per_10k_population": events_per_pop / 4.0,
        "absorption_per_10k_population": events_per_pop / 2.0,
        "change_of_use_per_10k_population": 0.0,
        "urban_inspections_per_10k_population": 0.0,
        "closed_locales_per_1k_population": 0.0,
        "vut_units_per_1k_population": 0.0,
        "official_change_of_use_24m": 0,
    }


def test_score_zone_rows_v2_exposes_relative_heat_and_uses_population_normalization() -> None:
    rows = score_zone_rows_v2(
        [
            _base_row("Zona Baja", events_per_pop=8.0, listings_per_pop=2.0),
            _base_row("Zona Alta", events_per_pop=30.0, listings_per_pop=6.0),
        ]
    )

    by_zone = {row["zone_label"]: row for row in rows}
    assert by_zone["Zona Alta"]["zone_relative_heat_score"] > by_zone["Zona Baja"]["zone_relative_heat_score"]
    assert by_zone["Zona Alta"]["zone_capture_score"] > by_zone["Zona Baja"]["zone_capture_score"]
    assert "evt/10k hab" in by_zone["Zona Alta"]["score_explanation"]


def test_score_zone_rows_v2_exposes_transformation_signal() -> None:
    low = _base_row("Zona Baja", events_per_pop=8.0, listings_per_pop=2.0)
    high = _base_row("Zona Transformacion", events_per_pop=8.0, listings_per_pop=2.0)
    high["change_of_use_per_10k_population"] = 4.5
    high["urban_inspections_per_10k_population"] = 12.0
    high["closed_locales_per_1k_population"] = 5.0
    high["official_change_of_use_24m"] = 9

    rows = score_zone_rows_v2([low, high])
    by_zone = {row["zone_label"]: row for row in rows}

    assert by_zone["Zona Transformacion"]["zone_transformation_signal_score"] > by_zone["Zona Baja"]["zone_transformation_signal_score"]
    assert "cambios de uso/24m" in by_zone["Zona Transformacion"]["score_explanation"]
