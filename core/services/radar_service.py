from sqlalchemy.orm import Session

from core.services.zone_intelligence_service import get_zone_intelligence


def get_radar_payload(session: Session) -> dict[str, list[dict]]:
    rows = get_zone_intelligence(session)

    top_opportunity = sorted(
        rows,
        key=lambda r: (r["zone_opportunity_score"], r["zone_activity_score"]),
        reverse=True,
    )[:12]

    top_saturation = sorted(
        rows,
        key=lambda r: (r["zone_saturation_score"], r["listings_count"]),
        reverse=True,
    )[:12]

    top_activity = sorted(
        rows,
        key=lambda r: (r["zone_activity_score"], r["telegram_occurrences_in_window"]),
        reverse=True,
    )[:12]

    return {
        "top_opportunity": top_opportunity,
        "top_saturation": top_saturation,
        "top_activity": top_activity,
    }