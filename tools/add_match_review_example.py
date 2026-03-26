from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from db.session import SessionLocal
from core.services.matching_metrics_service import add_match_review


def main() -> None:
    session = SessionLocal()
    try:
        add_match_review(
            session,
            review_label="match",
            source_channel="casafari",
            candidate_type="listing",
            raw_history_item_id=1,
            candidate_listing_id=1,
            predicted_score=0.93,
            predicted_status="resolved",
            review_reason="dirección, teléfono y precio coherentes",
            reviewer="manual",
        )

        add_match_review(
            session,
            review_label="no_match",
            source_channel="casafari",
            candidate_type="listing",
            raw_history_item_id=2,
            candidate_listing_id=5,
            predicted_score=0.78,
            predicted_status="resolved",
            review_reason="mismo portal pero activo distinto",
            reviewer="manual",
        )

        session.commit()
        print("Ejemplos de revisión insertados")
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    main()