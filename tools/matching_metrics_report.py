from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from db.session import SessionLocal
from core.services.matching_metrics_service import (
    get_matching_metrics,
    suggest_threshold_diagnostics,
)


def main() -> None:
    session = SessionLocal()
    try:
        metrics = get_matching_metrics(session)
        bands = suggest_threshold_diagnostics(session)

        print("=== Matching metrics ===")
        for key in [
            "reviews_total",
            "positive_reviews",
            "negative_reviews",
            "uncertain_reviews",
            "accepted_predictions",
            "rejected_predictions",
            "true_positive",
            "false_positive",
            "false_negative",
            "true_negative",
            "precision",
            "recall",
            "accuracy",
            "avg_predicted_score",
        ]:
            print(f"{key}: {metrics.get(key)}")

        print("\n=== Label counter ===")
        print(metrics.get("label_counter"))

        print("\n=== Status counter ===")
        print(metrics.get("status_counter"))

        print("\n=== Source counter ===")
        print(metrics.get("source_counter"))

        print("\n=== Threshold diagnostics ===")
        for row in bands:
            print(row)

    finally:
        session.close()


if __name__ == "__main__":
    main()