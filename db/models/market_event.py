from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base, utc_now


class MarketEvent(Base):
    __tablename__ = "market_events"

    id: Mapped[int] = mapped_column(primary_key=True)

    asset_id: Mapped[int | None] = mapped_column(ForeignKey("assets.id"), index=True)
    listing_id: Mapped[int | None] = mapped_column(ForeignKey("listings.id"), index=True)

    event_type: Mapped[str] = mapped_column(String(50), index=True)
    event_datetime: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

    price_old: Mapped[float | None] = mapped_column(Float)
    price_new: Mapped[float | None] = mapped_column(Float)

    status_old: Mapped[str | None] = mapped_column(String(50))
    status_new: Mapped[str | None] = mapped_column(String(50))

    source_channel: Mapped[str | None] = mapped_column(String(50), index=True)
    raw_text: Mapped[str | None] = mapped_column(Text)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)

    asset = relationship("Asset", back_populates="market_events")
    listing = relationship("Listing", back_populates="market_events")