from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from db.base import Base, utc_now


class TelegramAlert(Base):
    __tablename__ = "telegram_alerts"

    id: Mapped[int] = mapped_column(primary_key=True)

    message_key: Mapped[str] = mapped_column(String(120), index=True)
    canonical_key: Mapped[str] = mapped_column(String(120), unique=True, index=True)

    source_file: Mapped[str | None] = mapped_column(String(255), index=True)
    external_message_id: Mapped[str | None] = mapped_column(String(100), index=True)

    message_datetime: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    latest_message_datetime: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)

    occurrence_count: Mapped[int] = mapped_column(Integer, default=1)

    event_type_guess: Mapped[str | None] = mapped_column(String(50), index=True)

    property_type_raw: Mapped[str | None] = mapped_column(String(100))
    address_raw: Mapped[str | None] = mapped_column(String(255))
    price_eur: Mapped[float | None] = mapped_column(Float)
    price_per_m2: Mapped[float | None] = mapped_column(Float)
    area_m2: Mapped[float | None] = mapped_column(Float)
    bedrooms: Mapped[int | None] = mapped_column(Integer)
    bathrooms: Mapped[int | None] = mapped_column(Integer)

    listing_url: Mapped[str | None] = mapped_column(String(500), index=True)
    source_portal: Mapped[str | None] = mapped_column(String(100), index=True)

    contact_phone_raw: Mapped[str | None] = mapped_column(String(50))
    contact_name_raw: Mapped[str | None] = mapped_column(String(255))
    owner_listing_count: Mapped[int | None] = mapped_column(Integer)
    alert_name_raw: Mapped[str | None] = mapped_column(String(255))

    raw_text: Mapped[str | None] = mapped_column(Text)

    asset_id: Mapped[int | None] = mapped_column(ForeignKey("assets.id"), index=True)
    listing_id: Mapped[int | None] = mapped_column(ForeignKey("listings.id"), index=True)

    resolved: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    resolution_note: Mapped[str | None] = mapped_column(String(255))
    resolution_strategy: Mapped[str | None] = mapped_column(String(100), index=True)

    matched_existing_listing: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    matched_existing_asset: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    created_new_listing: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    created_new_asset: Mapped[bool] = mapped_column(Boolean, default=False, index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )