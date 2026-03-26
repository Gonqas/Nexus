from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.services.casafari_links_service import (
    get_casafari_matching_review_summary,
    save_casafari_match_review,
)
from core.services.matching_metrics_service import (
    add_match_review,
    get_latest_match_review_map,
)
from db.base import Base
import db.models  # noqa: F401


def make_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    return Session()


def test_latest_match_review_map_returns_newest_review_per_raw() -> None:
    session = make_session()
    try:
        add_match_review(
            session,
            review_label="uncertain",
            raw_history_item_id=10,
            predicted_status="ambiguous",
            review_reason="primera pasada",
            reviewer="ana",
        )
        add_match_review(
            session,
            review_label="match",
            raw_history_item_id=10,
            predicted_status="resolved",
            review_reason="revision final",
            reviewer="mario",
        )
        session.commit()

        latest = get_latest_match_review_map(session, [10])
        assert latest[10]["review_label"] == "match"
        assert latest[10]["review_reason"] == "revision final"
        assert latest[10]["reviewer"] == "mario"
    finally:
        session.close()


def test_casafari_review_summary_tracks_precision_recall_and_counts() -> None:
    session = make_session()
    try:
        save_casafari_match_review(
            session,
            raw_history_item_id=11,
            listing_id=21,
            asset_id=31,
            review_label="match",
            review_reason="correcto",
            reviewer="ana",
            predicted_status="resolved",
            predicted_score=92.0,
        )
        save_casafari_match_review(
            session,
            raw_history_item_id=12,
            listing_id=None,
            asset_id=None,
            review_label="match",
            review_reason="el sistema lo dejo sin resolver",
            reviewer="ana",
            predicted_status="unresolved",
            predicted_score=0.0,
        )

        summary = get_casafari_matching_review_summary(session)
        metrics = summary["metrics"]

        assert metrics["reviews_total"] == 2
        assert metrics["true_positive"] == 1
        assert metrics["false_negative"] == 1
        assert metrics["precision"] == 1.0
        assert metrics["recall"] == 0.5
    finally:
        session.close()
