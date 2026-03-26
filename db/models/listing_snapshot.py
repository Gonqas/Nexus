from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base, utc_now


class ListingSnapshot(Base):
    __tablename__ = "listing_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True)

    listing_id: Mapped[int] = mapped_column(ForeignKey("listings.id"), index=True)
    contact_id: Mapped[int | None] = mapped_column(ForeignKey("contacts.id"), index=True)

    snapshot_datetime: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    source_channel: Mapped[str] = mapped_column(String(50), index=True)

    price_eur: Mapped[float | None] = mapped_column(Float)
    status: Mapped[str | None] = mapped_column(String(50), index=True)

    raw_payload: Mapped[str | None] = mapped_column(Text)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)

    listing = relationship("Listing", back_populates="snapshots")