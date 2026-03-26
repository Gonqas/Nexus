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


def recommend_action(opportunity_score: float, saturation_score: float, activity_score: float) -> str:
    if opportunity_score >= 70 and saturation_score <= 60:
        return "Atacar ya"
    if saturation_score >= 75 and activity_score <= 40:
        return "Zona quemada"
    if activity_score >= 60 and saturation_score < 75:
        return "Vigilar"
    if activity_score < 25 and saturation_score < 40:
        return "Baja prioridad"
    return "Seguir observando"


def score_zone_rows(rows: list[dict]) -> list[dict]:
    if not rows:
        return rows

    listings_counts = [float(r["listings_count"]) for r in rows]
    listings_per_asset = [float(r["listings_per_asset"]) for r in rows]
    contacts_counts = [float(r["contacts_count"]) for r in rows]
    alerts_in_window = [float(r["telegram_alerts_in_window"]) for r in rows]
    occurrences_in_window = [float(r["telegram_occurrences_in_window"]) for r in rows]
    resolved_in_window = [float(r["telegram_resolved_in_window"]) for r in rows]
    diversity = [float(r["asset_type_diversity"]) for r in rows]

    norm_listings = _normalize(listings_counts)
    norm_lpa = _normalize(listings_per_asset)
    norm_contacts = _normalize(contacts_counts)
    norm_alerts = _normalize(alerts_in_window)
    norm_occ = _normalize(occurrences_in_window)
    norm_resolved = _normalize(resolved_in_window)
    norm_diversity = _normalize(diversity)

    result: list[dict] = []

    for idx, row in enumerate(rows):
        saturation = (
            0.55 * norm_listings[idx]
            + 0.25 * norm_lpa[idx]
            + 0.20 * norm_contacts[idx]
        )

        activity = (
            0.50 * norm_alerts[idx]
            + 0.35 * norm_occ[idx]
            + 0.15 * norm_resolved[idx]
        )

        confidence = _safe_ratio(
            float(row["telegram_resolved_in_window"]),
            float(row["telegram_alerts_in_window"]),
        )

        opportunity = (
            0.45 * activity
            + 0.25 * (1 - saturation)
            + 0.15 * norm_diversity[idx]
            + 0.15 * confidence
        )

        row = dict(row)
        row["zone_saturation_score"] = round(saturation * 100, 1)
        row["zone_activity_score"] = round(activity * 100, 1)
        row["zone_opportunity_score"] = round(opportunity * 100, 1)
        row["confidence_ratio"] = round(confidence * 100, 1)
        row["recommended_action"] = recommend_action(
            row["zone_opportunity_score"],
            row["zone_saturation_score"],
            row["zone_activity_score"],
        )
        result.append(row)

    result.sort(
        key=lambda r: (r["zone_opportunity_score"], r["zone_activity_score"]),
        reverse=True,
    )
    return result