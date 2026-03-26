from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import core.services.casafari_sync_service as casafari_sync_service
from db.base import Base
import db.models  # noqa: F401


def make_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    return Session()


def test_get_sync_status_exposes_debug_summary(monkeypatch) -> None:
    session = make_session()
    try:
        monkeypatch.setattr(
            casafari_sync_service,
            "get_latest_casafari_debug_summary",
            lambda: {
                "debug_dir": "C:/debug/latest",
                "extractor_used": "network",
                "pages_seen": 3,
                "total_expected": 28,
                "coverage_gap": 2,
                "candidate_payload_count": 8,
                "captured_payload_count": 10,
                "warning_count": 1,
                "warnings": ["gap"],
                "sync_mode": "fast",
                "final_url": "https://example.com/final",
                "target_url": "https://example.com/target",
            },
        )
        monkeypatch.setattr(
            casafari_sync_service,
            "get_casafari_session_status",
            lambda: {
                "session_exists": True,
                "session_ready": True,
                "session_file": "C:/state/casafari_storage_state.json",
                "session_saved_at": "2026-03-26T10:00:00",
                "verified_history_url": "https://example.com/history",
                "verified_history_saved_at": "2026-03-26T10:00:05",
            },
        )

        status = casafari_sync_service.get_sync_status(session)

        assert status["last_debug_dir"] == "C:/debug/latest"
        assert status["last_extractor_used"] == "network"
        assert status["last_pages_seen"] == 3
        assert status["last_sync_mode"] == "fast"
        assert status["last_warning_count"] == 1
        assert status["session_ready"] is True
        assert status["verified_history_url"] == "https://example.com/history"
    finally:
        session.close()
