from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from core.config.settings import CASAFARI_DEBUG_BASE_DIR


def _summary_files() -> list[Path]:
    if not CASAFARI_DEBUG_BASE_DIR.exists():
        return []
    return sorted(
        CASAFARI_DEBUG_BASE_DIR.glob("*/run_summary.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )


def get_latest_casafari_debug_summary() -> dict[str, Any]:
    for path in _summary_files():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue

        warnings = data.get("warnings") or []
        total_expected = data.get("total_expected")
        items_seen = data.get("items_seen")
        coverage_gap = data.get("coverage_gap")
        if coverage_gap is None and isinstance(total_expected, int) and isinstance(items_seen, int):
            coverage_gap = max(total_expected - items_seen, 0)

        return {
            "debug_dir": str(path.parent),
            "run_summary_path": str(path),
            "target_url": data.get("target_url"),
            "final_url": data.get("final_url"),
            "pages_seen": data.get("pages_seen"),
            "items_seen": data.get("items_seen"),
            "total_expected": total_expected,
            "coverage_gap": coverage_gap,
            "extractor_used": data.get("extractor_used"),
            "candidate_payload_count": data.get("candidate_payloads"),
            "captured_payload_count": data.get("captured_payloads_total"),
            "warnings": warnings,
            "warning_count": len(warnings),
            "sync_mode": data.get("sync_mode"),
        }

    return {
        "debug_dir": None,
        "run_summary_path": None,
        "target_url": None,
        "final_url": None,
        "pages_seen": None,
        "items_seen": None,
        "total_expected": None,
        "coverage_gap": None,
        "extractor_used": None,
        "candidate_payload_count": None,
        "captured_payload_count": None,
        "warnings": [],
        "warning_count": 0,
        "sync_mode": None,
    }
