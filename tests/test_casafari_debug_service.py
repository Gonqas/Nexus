import json
from pathlib import Path

import core.services.casafari_debug_service as casafari_debug_service


def test_get_latest_casafari_debug_summary_reads_newest_run(monkeypatch, tmp_path: Path) -> None:
    older_dir = tmp_path / "20260326_100000"
    newer_dir = tmp_path / "20260326_110000"
    older_dir.mkdir()
    newer_dir.mkdir()

    (older_dir / "run_summary.json").write_text(
        json.dumps({"items_seen": 10, "warnings": []}),
        encoding="utf-8",
    )
    (newer_dir / "run_summary.json").write_text(
        json.dumps(
            {
                "target_url": "https://example.com/history",
                "final_url": "https://example.com/history?page=2",
                "pages_seen": 2,
                "items_seen": 18,
                "total_expected": 25,
                "extractor_used": "network",
                "candidate_payloads": 4,
                "captured_payloads_total": 7,
                "warnings": ["gap"],
                "sync_mode": "fast",
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(casafari_debug_service, "CASAFARI_DEBUG_BASE_DIR", tmp_path)

    summary = casafari_debug_service.get_latest_casafari_debug_summary()

    assert summary["debug_dir"] == str(newer_dir)
    assert summary["pages_seen"] == 2
    assert summary["coverage_gap"] == 7
    assert summary["warning_count"] == 1
    assert summary["sync_mode"] == "fast"


def test_get_latest_casafari_debug_summary_handles_missing_data(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(casafari_debug_service, "CASAFARI_DEBUG_BASE_DIR", tmp_path)

    summary = casafari_debug_service.get_latest_casafari_debug_summary()

    assert summary["debug_dir"] is None
    assert summary["warnings"] == []
