from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from db.base import Base


def utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class MatchReview(Base):
    __tablename__ = "match_review"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    source_channel: Mapped[str | None] = mapped_column(String(50), nullable=True)
    candidate_type: Mapped[str | None] = mapped_column(String(50), nullable=True)

    raw_history_item_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("raw_history_item.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    listing_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("listing.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    asset_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("asset.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    candidate_listing_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("listing.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    candidate_asset_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("asset.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    predicted_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    predicted_status: Mapped[str | None] = mapped_column(String(30), nullable=True)

    review_label: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    review_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    reviewer: Mapped[str | None] = mapped_column(String(120), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=utc_now_naive,
        index=True,
    )