from sqlalchemy.orm import Session

from core.services.ai_explanations_service import explain_zone_row
from core.services.predictive_signal_service import build_zone_prediction
from core.features.zone_features_v2 import build_zone_feature_rows_v2
from core.scoring.zone_scoring_v2 import score_zone_rows_v2


def build_zone_executive_summary(row: dict) -> str:
    bits: list[str] = []

    if row["zone_heat_score"] >= 65:
        bits.append("actividad reciente alta")
    elif row["zone_heat_score"] >= 45:
        bits.append("actividad reciente media")
    else:
        bits.append("actividad reciente baja")

    if row.get("zone_relative_heat_score", 0) >= 65:
        bits.append("actividad relativa alta para su tamano")
    elif row.get("zone_relative_heat_score", 0) >= 45:
        bits.append("actividad relativa media para su tamano")

    if row.get("zone_transformation_signal_score", 0) >= 65:
        bits.append("senal transformadora alta")
    elif row.get("zone_transformation_signal_score", 0) >= 45:
        bits.append("senal transformadora media")

    if row["zone_pressure_score"] >= 65:
        bits.append("presión comercial relevante")
    elif row["zone_pressure_score"] >= 45:
        bits.append("presión moderada")

    if row["zone_liquidity_score"] >= 60:
        bits.append("buena salida de producto")
    elif row["zone_liquidity_score"] < 35:
        bits.append("liquidez débil")

    if row.get("geo_point_ratio", 0) >= 0.75:
        bits.append("cobertura geográfica alta")
    elif row.get("geo_point_ratio", 0) >= 0.40:
        bits.append("cobertura geográfica media")
    else:
        bits.append("cobertura geográfica baja")

    if row["zone_confidence_score"] < 40:
        bits.append("confianza baja")
    elif row["zone_confidence_score"] >= 60:
        bits.append("lectura con confianza razonable")

    population = row.get("official_population")
    if population:
        bits.append(f"{float(row.get('events_14d_per_10k_population') or 0.0):.1f} eventos por 10k hab")

    vulnerability = row.get("official_vulnerability_index")
    if vulnerability is not None:
        bits.append(f"IVT {float(vulnerability):.1f}")
    if row.get("official_change_of_use_24m"):
        bits.append(f"{int(row.get('official_change_of_use_24m') or 0)} cambios de uso recientes")
    if row.get("predicted_absorption_30d_score") is not None:
        bits.append(
            f"prediccion 30d {float(row.get('predicted_absorption_30d_score') or 0.0):.1f}"
        )

    return ". ".join(bits).capitalize() + "." if bits else "Sin lectura ejecutiva suficiente."


def get_zone_intelligence_v2(session: Session, window_days: int = 14) -> list[dict]:
    rows = build_zone_feature_rows_v2(session, window_days=window_days)
    rows = [
        row for row in rows
        if row["assets_count"] > 0
        or row["events_14d"] > 0
        or row["casafari_raw_in_zone"] > 0
    ]
    rows = score_zone_rows_v2(rows)

    enriched = []
    for row in rows:
        row = dict(row)
        row.update(build_zone_prediction(row))
        legacy_summary = build_zone_executive_summary(row)
        explanation = explain_zone_row(row)
        row["legacy_executive_summary"] = legacy_summary
        row["executive_summary"] = explanation["ai_summary"]
        row.update(explanation)
        enriched.append(row)

    return enriched
