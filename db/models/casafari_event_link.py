from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base, utc_now


class CasafariEventLink(Base):
    __tablename__ = "casafari_event_links"

    id: Mapped[int] = mapped_column(primary_key=True)

    raw_history_item_id: Mapped[int] = mapped_column(
        ForeignKey("raw_history_items.id"),
        unique=True,
        index=True,
    )
    listing_id: Mapped[int | None] = mapped_column(ForeignKey("listings.id"), index=True)
    asset_id: Mapped[int | None] = mapped_column(ForeignKey("assets.id"), index=True)
    contact_id: Mapped[int | None] = mapped_column(ForeignKey("contacts.id"), index=True)
    market_event_id: Mapped[int | None] = mapped_column(ForeignKey("market_events.id"), index=True)

    match_status: Mapped[str | None] = mapped_column(String(50), index=True)
    match_strategy: Mapped[str | None] = mapped_column(String(100), index=True)
    match_score: Mapped[float | None] = mapped_column(Float)
    match_note: Mapped[str | None] = mapped_column(String(500))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )

    raw_history_item = relationship("RawHistoryItem")
    listing = relationship("Listing")
    asset = relationship("Asset")
    contact = relationship("Contact")
    market_event = relationship("MarketEvent")