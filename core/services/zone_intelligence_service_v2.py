from sqlalchemy.orm import Session

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
        row["executive_summary"] = build_zone_executive_summary(row)
        enriched.append(row)

    return enriched