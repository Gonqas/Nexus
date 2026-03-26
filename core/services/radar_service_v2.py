from sqlalchemy.orm import Session

from core.services.zone_intelligence_service_v2 import get_zone_intelligence_v2


def get_radar_payload_v2(session: Session, window_days: int = 14) -> dict[str, list[dict]]:
    rows = get_zone_intelligence_v2(session, window_days=window_days)

    top_capture = sorted(
        rows,
        key=lambda r: (r["zone_capture_score"], r["zone_confidence_score"]),
        reverse=True,
    )[:12]

    top_heat = sorted(
        rows,
        key=lambda r: (r["zone_heat_score"], r["events_14d"]),
        reverse=True,
    )[:12]

    top_pressure = sorted(
        rows,
        key=lambda r: (r["zone_pressure_score"], r["price_drop_count"]),
        reverse=True,
    )[:12]

    top_liquidity = sorted(
        rows,
        key=lambda r: (r["zone_liquidity_score"], r["absorption_count"]),
        reverse=True,
    )[:12]

    low_confidence = sorted(
        rows,
        key=lambda r: (r["zone_confidence_score"], -r["casafari_raw_in_zone"]),
    )[:12]

    return {
        "top_capture": top_capture,
        "top_heat": top_heat,
        "top_pressure": top_pressure,
        "top_liquidity": top_liquidity,
        "low_confidence": low_confidence,
    }