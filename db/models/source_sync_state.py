from datetime import datetime

from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from db.base import Base, utc_now


class SourceSyncState(Base):
    __tablename__ = "source_sync_states"

    id: Mapped[int] = mapped_column(primary_key=True)
    source_name: Mapped[str] = mapped_column(String(100), unique=True, index=True)

    last_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_success_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_success_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    last_status: Mapped[str | None] = mapped_column(String(50), index=True)
    last_message: Mapped[str | None] = mapped_column(String(500))
    last_item_count: Mapped[int | None] = mapped_column(Integer)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )