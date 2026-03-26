def _normalize(values: list[float]) -> list[float]:
    if not values:
        return []

    min_v = min(values)
    max_v = max(values)

    if max_v == min_v:
        return [0.5 for _ in values]

    return [(v - min_v) / (max_v - min_v) for v in values]


def _safe_ratio(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return a / b


def _freshness_score(days_old: int | None) -> float:
    if days_old is None:
        return 0.15
    if days_old <= 3:
        return 1.0
    if days_old <= 7:
        return 0.8
    if days_old <= 14:
        return 0.6
    if days_old <= 30:
        return 0.35
    return 0.15


def recommend_action_v2(capture: float, pressure: float, liquidity: float, confidence: float) -> str:
    if confidence < 40:
        return "Poca senal / baja confianza"
    if capture >= 75 and pressure >= 55:
        return "Captacion agresiva"
    if capture >= 60:
        return "Captacion selectiva"
    if pressure >= 70 and liquidity >= 50:
        return "Negociacion mas que captacion"
    if pressure >= 75 and liquidity < 40:
        return "Zona saturada"
    if capture >= 45 or liquidity >= 50:
        return "Seguir y vigilar"
    return "Baja prioridad"


def build_score_explanation_v2(row: dict) -> str:
    bits: list[str] = []

    if row["zone_heat_score"] >= 65:
        bits.append("actividad alta")
    elif row["zone_heat_score"] >= 45:
        bits.append("actividad media")

    if row.get("zone_relative_heat_score", 0) >= 65:
        bits.append("actividad relativa alta")
    elif row.get("zone_relative_heat_score", 0) >= 45:
        bits.append("actividad relativa media")

    if row.get("zone_transformation_signal_score", 0) >= 65:
        bits.append("senal de transformacion alta")
    elif row.get("zone_transformation_signal_score", 0) >= 45:
        bits.append("senal de transformacion media")

    if row["zone_pressure_score"] >= 65:
        bits.append("presion alta")
    elif row["zone_pressure_score"] >= 45:
        bits.append("presion moderada")

    if row["zone_liquidity_score"] >= 60:
        bits.append("liquidez fuerte")
    elif row["zone_liquidity_score"] < 35:
        bits.append("liquidez debil")

    if row.get("geo_point_ratio", 0) >= 0.75:
        bits.append("geo precisa")
    elif row.get("geo_point_ratio", 0) >= 0.40:
        bits.append("geo aceptable")
    else:
        bits.append("geo floja")

    if row["zone_confidence_score"] < 40:
        bits.append("confianza baja")
    elif row["zone_confidence_score"] >= 60:
        bits.append("confianza razonable")

    if row.get("official_population"):
        bits.append(f"{float(row.get('events_14d_per_10k_population') or 0.0):.1f} evt/10k hab")
    if row.get("official_change_of_use_24m"):
        bits.append(f"{int(row.get('official_change_of_use_24m') or 0)} cambios de uso/24m")

    return " | ".join(bits) if bits else "Sin explicacion suficiente"


def score_zone_rows_v2(rows: list[dict]) -> list[dict]:
    if not rows:
        return rows

    active_listings = [float(r["active_listings_count"]) for r in rows]
    listings_per_asset = [float(r["listings_per_asset"]) for r in rows]
    broker_share = [float(r["broker_phone_share"]) for r in rows]
    events_14d = [float(r["events_14d"]) for r in rows]
    new_supply = [float(r["listing_detected_count"]) for r in rows]
    price_drops = [float(r["price_drop_count"]) for r in rows]
    absorption = [float(r["absorption_count"]) for r in rows]
    resolved_ratio = [float(r["resolved_ratio"]) for r in rows]
    asset_diversity = [float(r["asset_type_diversity"]) for r in rows]
    portal_diversity = [float(r["portal_diversity"]) for r in rows]
    events_per_10k_population = [float(r.get("events_14d_per_10k_population") or 0.0) for r in rows]
    listings_per_1k_population = [float(r.get("active_listings_per_1k_population") or 0.0) for r in rows]
    drops_per_10k_population = [float(r.get("price_drop_per_10k_population") or 0.0) for r in rows]
    absorption_per_10k_population = [float(r.get("absorption_per_10k_population") or 0.0) for r in rows]
    change_of_use_per_10k_population = [float(r.get("change_of_use_per_10k_population") or 0.0) for r in rows]
    inspections_per_10k_population = [float(r.get("urban_inspections_per_10k_population") or 0.0) for r in rows]
    closed_locales_per_1k_population = [float(r.get("closed_locales_per_1k_population") or 0.0) for r in rows]
    vut_units_per_1k_population = [float(r.get("vut_units_per_1k_population") or 0.0) for r in rows]
    context_coverage = [1.0 if r.get("official_population") else 0.0 for r in rows]
    geo_point_ratio_raw = [
        _safe_ratio(float(r["geo_point_assets"]), float(r["assets_count"]))
        for r in rows
    ]
    geo_neighborhood_ratio_raw = [
        _safe_ratio(float(r["geo_neighborhood_assets"]), float(r["assets_count"]))
        for r in rows
    ]

    norm_active = _normalize(active_listings)
    norm_lpa = _normalize(listings_per_asset)
    norm_broker = _normalize(broker_share)
    norm_events = _normalize(events_14d)
    norm_new_supply = _normalize(new_supply)
    norm_drops = _normalize(price_drops)
    norm_absorption = _normalize(absorption)
    norm_resolved = _normalize(resolved_ratio)
    norm_asset_div = _normalize(asset_diversity)
    norm_portal_div = _normalize(portal_diversity)
    norm_events_per_pop = _normalize(events_per_10k_population)
    norm_listings_per_pop = _normalize(listings_per_1k_population)
    norm_drops_per_pop = _normalize(drops_per_10k_population)
    norm_absorption_per_pop = _normalize(absorption_per_10k_population)
    norm_change_of_use = _normalize(change_of_use_per_10k_population)
    norm_inspections = _normalize(inspections_per_10k_population)
    norm_closed_locales = _normalize(closed_locales_per_1k_population)
    norm_vut = _normalize(vut_units_per_1k_population)
    norm_context = _normalize(context_coverage)
    norm_geo_point = _normalize(geo_point_ratio_raw)
    norm_geo_neighborhood = _normalize(geo_neighborhood_ratio_raw)

    result: list[dict] = []

    for idx, row in enumerate(rows):
        freshness = _freshness_score(row["csv_freshness_days"])

        saturation = (
            0.45 * norm_active[idx]
            + 0.30 * norm_lpa[idx]
            + 0.25 * norm_broker[idx]
        )

        heat = (
            0.28 * norm_events[idx]
            + 0.22 * norm_new_supply[idx]
            + 0.18 * norm_absorption[idx]
            + 0.20 * norm_events_per_pop[idx]
            + 0.12 * norm_absorption_per_pop[idx]
        )

        relative_heat = (
            0.40 * norm_events_per_pop[idx]
            + 0.30 * norm_absorption_per_pop[idx]
            + 0.30 * norm_listings_per_pop[idx]
        )

        transformation = (
            0.38 * norm_change_of_use[idx]
            + 0.26 * norm_inspections[idx]
            + 0.24 * norm_closed_locales[idx]
            + 0.12 * norm_vut[idx]
        )

        pressure = (
            0.25 * norm_drops[idx]
            + 0.20 * norm_drops_per_pop[idx]
            + 0.20 * saturation
            + 0.15 * norm_broker[idx]
            + 0.10 * max(norm_new_supply[idx] - norm_absorption[idx], 0.0)
            + 0.10 * transformation
        )

        liquidity_raw = (
            0.50 * _safe_ratio(float(row["absorption_count"]), float(row["listing_detected_count"]) + 1.0)
            + 0.30 * norm_absorption[idx]
            + 0.20 * norm_events[idx]
        )
        liquidity = min(max(liquidity_raw, 0.0), 1.0)

        geo_quality = (
            0.60 * norm_geo_point[idx]
            + 0.40 * norm_geo_neighborhood[idx]
        )

        confidence = (
            0.32 * norm_resolved[idx]
            + 0.18 * freshness
            + 0.15 * norm_asset_div[idx]
            + 0.10 * norm_portal_div[idx]
            + 0.20 * geo_quality
            + 0.05 * norm_context[idx]
        )

        capture = (
            0.24 * heat
            + 0.18 * relative_heat
            + 0.22 * pressure
            + 0.12 * liquidity
            + 0.14 * confidence
            + 0.10 * transformation
        )

        row = dict(row)
        row["geo_point_ratio"] = round(geo_point_ratio_raw[idx], 4)
        row["geo_neighborhood_ratio"] = round(geo_neighborhood_ratio_raw[idx], 4)
        row["zone_saturation_score"] = round(saturation * 100, 1)
        row["zone_heat_score"] = round(heat * 100, 1)
        row["zone_relative_heat_score"] = round(relative_heat * 100, 1)
        row["zone_transformation_signal_score"] = round(transformation * 100, 1)
        row["zone_pressure_score"] = round(pressure * 100, 1)
        row["zone_liquidity_score"] = round(liquidity * 100, 1)
        row["zone_capture_score"] = round(capture * 100, 1)
        row["zone_confidence_score"] = round(confidence * 100, 1)
        row["score_explanation"] = build_score_explanation_v2(row)
        row["recommended_action"] = recommend_action_v2(
            row["zone_capture_score"],
            row["zone_pressure_score"],
            row["zone_liquidity_score"],
            row["zone_confidence_score"],
        )
        result.append(row)

    result.sort(
        key=lambda r: (
            r["zone_capture_score"],
            r["zone_relative_heat_score"],
            r["zone_confidence_score"],
            r["zone_heat_score"],
        ),
        reverse=True,
    )
    return result
