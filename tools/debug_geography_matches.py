from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from sqlalchemy import select
from sqlalchemy.orm import joinedload

from db.session import SessionLocal
from db.models.asset import Asset
from core.normalization.addresses import get_madrid_street_catalog


def best_address(asset: Asset) -> str | None:
    if asset.address_raw:
        return asset.address_raw
    if asset.building and asset.building.address_base:
        return asset.building.address_base
    return None


def main() -> None:
    session = SessionLocal()
    try:
        catalog = get_madrid_street_catalog()
        if catalog is None:
            print("No se ha encontrado el catálogo de calles.")
            return

        assets = list(
            session.scalars(
                select(Asset).options(joinedload(Asset.building)).order_by(Asset.id.asc())
            ).unique().all()
        )

        matched = 0
        unmatched_rows = []

        for asset in assets:
            address_text = best_address(asset)
            match = catalog.resolve(address_text)

            if match.matched:
                matched += 1
            else:
                unmatched_rows.append(
                    {
                        "asset_id": asset.id,
                        "address": address_text,
                        "match_type": match.match_type,
                    }
                )

        print(f"assets_total={len(assets)}")
        print(f"assets_matched={matched}")
        print(f"assets_unmatched={len(unmatched_rows)}")
        print()
        print("Primeros 60 no resueltos:")
        for row in unmatched_rows[:60]:
            print(f"[{row['asset_id']}] {row['address']} -> {row['match_type']}")

    finally:
        session.close()


if __name__ == "__main__":
    main()