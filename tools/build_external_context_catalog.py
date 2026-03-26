from __future__ import annotations

import sys
from pathlib import Path

import orjson

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from core.discovery.external_context_catalog import harvest_catalog


RAW_DIR = BASE_DIR / "data" / "raw" / "external_context"
PROCESSED_DIR = BASE_DIR / "data" / "processed"


def ensure_dirs() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: dict) -> None:
    path.write_bytes(orjson.dumps(payload, option=orjson.OPT_INDENT_2))


def main() -> None:
    ensure_dirs()

    raw_by_portal, full_catalog, focus_catalog, summary = harvest_catalog()

    for portal_id, payload in raw_by_portal.items():
        write_json(RAW_DIR / f"{portal_id}_packages_raw.json", {"datasets": payload})

    write_json(PROCESSED_DIR / "madrid_external_context_catalog_full.json", full_catalog)
    write_json(PROCESSED_DIR / "madrid_external_context_catalog_focus.json", focus_catalog)
    write_json(PROCESSED_DIR / "madrid_external_context_catalog_summary.json", summary)

    print("Catalogo externo generado")
    print(f"  full_datasets={full_catalog['dataset_count']}")
    print(f"  focus_datasets={focus_catalog['dataset_count']}")
    for portal_id, portal_summary in summary["portals"].items():
        print(
            f"  {portal_id}: total={portal_summary['dataset_count']} focus={portal_summary['focus_count']}"
        )


if __name__ == "__main__":
    main()
