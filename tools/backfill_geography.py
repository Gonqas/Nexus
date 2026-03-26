from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from db.session import SessionLocal
from core.services.geography_enrichment_service import (
    backfill_assets_geography,
    backfill_buildings_geography,
)


def main() -> None:
    session = SessionLocal()
    try:
        building_stats = backfill_buildings_geography(
            session=session,
            only_missing=False,
        )
        asset_stats = backfill_assets_geography(
            session=session,
            only_missing=False,
        )

        session.commit()

        print("Backfill completado")
        print(building_stats)
        print(asset_stats)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    main()