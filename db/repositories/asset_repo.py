from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from db.models.asset import Asset


def get_assets_with_relations(session: Session, limit: int = 300) -> list[Asset]:
    stmt = (
        select(Asset)
        .options(
            joinedload(Asset.building),
            joinedload(Asset.listings),
        )
        .order_by(Asset.id.desc())
        .limit(limit)
    )

    return list(session.scalars(stmt).unique().all())