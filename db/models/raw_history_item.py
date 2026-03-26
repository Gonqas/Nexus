from datetime import datetime

from sqlalchemy import DateTime, Float, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from db.base import Base, utc_now


class RawHistoryItem(Base):
    __tablename__ = "raw_history_items"

    id: Mapped[int] = mapped_column(primary_key=True)

    source_name: Mapped[str] = mapped_column(String(100), index=True)
    source_uid: Mapped[str] = mapped_column(String(120), unique=True, index=True)

    history_type: Mapped[str | None] = mapped_column(String(50), index=True)
    event_type_guess: Mapped[str | None] = mapped_column(String(50), index=True)

    event_datetime: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)

    title: Mapped[str | None] = mapped_column(String(500))
    address_raw: Mapped[str | None] = mapped_column(String(255))
    listing_url: Mapped[str | None] = mapped_column(String(500), index=True)
    portal: Mapped[str | None] = mapped_column(String(100), index=True)

    contact_name: Mapped[str | None] = mapped_column(String(255))
    contact_phone: Mapped[str | None] = mapped_column(String(50))

    current_price_eur: Mapped[float | None] = mapped_column(Float)
    previous_price_eur: Mapped[float | None] = mapped_column(Float)

    page_number: Mapped[int | None] = mapped_column()
    raw_text: Mapped[str | None] = mapped_column(Text)
    raw_payload_json: Mapped[str | None] = mapped_column(Text)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)