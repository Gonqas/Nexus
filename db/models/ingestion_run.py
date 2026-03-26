from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from db.base import Base, utc_now


class IngestionRun(Base):
    __tablename__ = "ingestion_runs"

    id: Mapped[int] = mapped_column(primary_key=True)

    source_type: Mapped[str] = mapped_column(String(50), index=True)
    source_name: Mapped[str | None] = mapped_column(String(100), index=True)

    file_name: Mapped[str | None] = mapped_column(String(500))
    original_path: Mapped[str | None] = mapped_column(String(1000))
    file_hash: Mapped[str | None] = mapped_column(String(128), index=True)

    status: Mapped[str | None] = mapped_column(String(50), index=True)
    message: Mapped[str | None] = mapped_column(String(500))
    error_text: Mapped[str | None] = mapped_column(Text)

    rows_read: Mapped[int | None] = mapped_column(Integer)
    contacts_processed: Mapped[int | None] = mapped_column(Integer)
    assets_processed: Mapped[int | None] = mapped_column(Integer)
    listings_created: Mapped[int | None] = mapped_column(Integer)
    snapshots_created: Mapped[int | None] = mapped_column(Integer)

    casafari_raw_items_processed: Mapped[int | None] = mapped_column(Integer)
    casafari_raw_items_resolved: Mapped[int | None] = mapped_column(Integer)
    casafari_raw_items_ambiguous: Mapped[int | None] = mapped_column(Integer)
    casafari_raw_items_unresolved: Mapped[int | None] = mapped_column(Integer)
    casafari_market_events_created: Mapped[int | None] = mapped_column(Integer)

    file_deleted: Mapped[bool] = mapped_column(Boolean, default=False)

    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=utc_now)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )