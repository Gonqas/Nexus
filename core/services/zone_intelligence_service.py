from sqlalchemy.orm import Session

from core.features.zone_features import build_zone_feature_rows
from core.scoring.zone_scoring import score_zone_rows


def get_zone_intelligence(session: Session) -> list[dict]:
    rows = build_zone_feature_rows(session)
    rows = [row for row in rows if row["assets_count"] > 0 or row["telegram_alerts_in_window"] > 0]
    return score_zone_rows(rows)