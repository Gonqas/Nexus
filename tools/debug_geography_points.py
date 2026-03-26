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
from core.geography.madrid_street_catalog import parse_address_text
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
            print("No se ha encontrado el catálogo.")
            return

        assets = list(
            session.scalars(
                select(Asset).options(joinedload(Asset.building)).order_by(Asset.id.asc())
            ).unique().all()
        )

        no_point = []

        for asset in assets:
            address = best_address(asset)
            parsed = parse_address_text(address)
            match = catalog.resolve(address)

            if not match.matched:
                continue

            if match.lat is None or match.lon is None:
                no_point.append(
                    {
                        "asset_id": asset.id,
                        "address": address,
                        "street_name": parsed.street_name,
                        "house_number": parsed.house_number,
                        "match_type": match.match_type,
                        "street_code": match.street_code,
                        "district": match.district,
                        "neighborhood": match.neighborhood,
                    }
                )

        print(f"matched_without_point={len(no_point)}")
        print()
        print("Primeros 80 matched sin coordenadas:")
        for row in no_point[:80]:
            print(
                f"[{row['asset_id']}] {row['address']} | "
                f"street={row['street_name']} | num={row['house_number']} | "
                f"type={row['match_type']} | code={row['street_code']} | "
                f"district={row['district']} | neighborhood={row['neighborhood']}"
            )

    finally:
        session.close()


if __name__ == "__main__":
    main()