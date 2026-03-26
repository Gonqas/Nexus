from __future__ import annotations

from collections import Counter, defaultdict
from statistics import mean
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from db.models.match_review import MatchReview


POSITIVE_LABELS = {"match", "same_asset", "same_listing"}
NEGATIVE_LABELS = {"no_match", "different"}
UNCERTAIN_LABELS = {"uncertain", "ambiguous", "skip"}


def _is_positive(label: str | None) -> bool:
    if not label:
        return False
    return label.strip().lower() in POSITIVE_LABELS


def _is_negative(label: str | None) -> bool:
    if not label:
        return False
    return label.strip().lower() in NEGATIVE_LABELS


def _is_uncertain(label: str | None) -> bool:
    if not label:
        return False
    return label.strip().lower() in UNCERTAIN_LABELS


def _safe_div(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return a / b


def add_match_review(
    session: Session,
    *,
    review_label: str,
    source_channel: str | None = None,
    candidate_type: str | None = None,
    raw_history_item_id: int | None = None,
    listing_id: int | None = None,
    asset_id: int | None = None,
    candidate_listing_id: int | None = None,
    candidate_asset_id: int | None = None,
    predicted_score: float | None = None,
    predicted_status: str | None = None,
    review_reason: str | None = None,
    reviewer: str | None = None,
) -> MatchReview:
    row = MatchReview(
        review_label=review_label,
        source_channel=source_channel,
        candidate_type=candidate_type,
        raw_history_item_id=raw_history_item_id,
        listing_id=listing_id,
        asset_id=asset_id,
        candidate_listing_id=candidate_listing_id,
        candidate_asset_id=candidate_asset_id,
        predicted_score=predicted_score,
        predicted_status=predicted_status,
        review_reason=review_reason,
        reviewer=reviewer,
    )
    session.add(row)
    session.flush()
    return row


def get_match_review_rows(session: Session) -> list[MatchReview]:
    return list(
        session.scalars(
            select(MatchReview).order_by(MatchReview.created_at.desc(), MatchReview.id.desc())
        ).all()
    )


def get_latest_match_review_map(
    session: Session,
    raw_history_item_ids: list[int],
) -> dict[int, dict[str, Any]]:
    if not raw_history_item_ids:
        return {}

    rows = list(
        session.scalars(
            select(MatchReview)
            .where(MatchReview.raw_history_item_id.in_(raw_history_item_ids))
            .order_by(MatchReview.created_at.desc(), MatchReview.id.desc())
        ).all()
    )

    latest: dict[int, dict[str, Any]] = {}
    for row in rows:
        if row.raw_history_item_id is None or row.raw_history_item_id in latest:
            continue

        latest[row.raw_history_item_id] = {
            "review_label": row.review_label,
            "review_reason": row.review_reason,
            "reviewer": row.reviewer,
            "created_at": row.created_at,
            "predicted_status": row.predicted_status,
            "predicted_score": row.predicted_score,
        }

    return latest


def get_matching_metrics(session: Session) -> dict[str, Any]:
    rows = get_match_review_rows(session)

    total = len(rows)
    positives = [r for r in rows if _is_positive(r.review_label)]
    negatives = [r for r in rows if _is_negative(r.review_label)]
    uncertain = [r for r in rows if _is_uncertain(r.review_label)]

    accepted = [
        r for r in rows
        if (r.predicted_status or "").lower() in {"resolved", "accepted", "match"}
    ]
    rejected = [
        r for r in rows
        if (r.predicted_status or "").lower() in {"unresolved", "rejected", "no_match", "ambiguous"}
    ]

    true_positive = sum(1 for r in accepted if _is_positive(r.review_label))
    false_positive = sum(1 for r in accepted if _is_negative(r.review_label))
    false_negative = sum(1 for r in rejected if _is_positive(r.review_label))
    true_negative = sum(1 for r in rejected if _is_negative(r.review_label))

    precision = _safe_div(true_positive, true_positive + false_positive)
    recall = _safe_div(true_positive, true_positive + false_negative)
    accuracy = _safe_div(true_positive + true_negative, true_positive + true_negative + false_positive + false_negative)

    predicted_scores = [r.predicted_score for r in rows if r.predicted_score is not None]

    label_counter = Counter((r.review_label or "").lower() for r in rows)
    status_counter = Counter((r.predicted_status or "").lower() for r in rows)
    source_counter = Counter((r.source_channel or "unknown").lower() for r in rows)
    reviewer_counter = Counter((r.reviewer or "unknown").lower() for r in rows)

    by_threshold_band: dict[str, dict[str, int]] = defaultdict(lambda: {"total": 0, "positive": 0, "negative": 0})

    for row in rows:
        score = row.predicted_score
        if score is None:
            band = "no_score"
        elif score >= 0.90:
            band = "0.90+"
        elif score >= 0.75:
            band = "0.75-0.89"
        elif score >= 0.60:
            band = "0.60-0.74"
        elif score >= 0.40:
            band = "0.40-0.59"
        else:
            band = "<0.40"

        by_threshold_band[band]["total"] += 1
        if _is_positive(row.review_label):
            by_threshold_band[band]["positive"] += 1
        elif _is_negative(row.review_label):
            by_threshold_band[band]["negative"] += 1

    return {
        "reviews_total": total,
        "positive_reviews": len(positives),
        "negative_reviews": len(negatives),
        "uncertain_reviews": len(uncertain),
        "accepted_predictions": len(accepted),
        "rejected_predictions": len(rejected),
        "true_positive": true_positive,
        "false_positive": false_positive,
        "false_negative": false_negative,
        "true_negative": true_negative,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "accuracy": round(accuracy, 4),
        "avg_predicted_score": round(mean(predicted_scores), 4) if predicted_scores else None,
        "label_counter": dict(label_counter),
        "status_counter": dict(status_counter),
        "source_counter": dict(source_counter),
        "reviewer_counter": dict(reviewer_counter),
        "threshold_bands": dict(by_threshold_band),
    }


def suggest_threshold_diagnostics(session: Session) -> list[dict[str, Any]]:
    rows = get_match_review_rows(session)

    bands = []
    metrics = get_matching_metrics(session).get("threshold_bands", {})
    order = ["0.90+", "0.75-0.89", "0.60-0.74", "0.40-0.59", "<0.40", "no_score"]

    for band in order:
        data = metrics.get(band)
        if not data:
            continue

        total = data["total"]
        positive = data["positive"]
        negative = data["negative"]
        purity = _safe_div(positive, positive + negative) if (positive + negative) else 0.0

        recommendation = "sin muestra"
        if total >= 5:
            if purity >= 0.9:
                recommendation = "banda muy fiable"
            elif purity >= 0.75:
                recommendation = "banda razonable"
            elif purity >= 0.5:
                recommendation = "revisar threshold"
            else:
                recommendation = "banda problemática"

        bands.append(
            {
                "band": band,
                "total": total,
                "positive": positive,
                "negative": negative,
                "purity": round(purity, 4),
                "recommendation": recommendation,
            }
        )

    return bands
