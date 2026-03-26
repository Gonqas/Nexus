from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base, utc_now


class Listing(Base):
    __tablename__ = "listings"

    id: Mapped[int] = mapped_column(primary_key=True)

    asset_id: Mapped[int | None] = mapped_column(ForeignKey("assets.id"), index=True)
    contact_id: Mapped[int | None] = mapped_column(ForeignKey("contacts.id"), index=True)

    source_portal: Mapped[str | None] = mapped_column(String(100), index=True)
    listing_url: Mapped[str | None] = mapped_column(String(500), index=True)
    property_url: Mapped[str | None] = mapped_column(String(500), index=True)
    external_id: Mapped[str | None] = mapped_column(String(100), index=True)

    first_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    status: Mapped[str | None] = mapped_column(String(50), index=True)

    price_eur: Mapped[float | None] = mapped_column(Float)
    price_per_m2: Mapped[float | None] = mapped_column(Float)
    area_m2: Mapped[float | None] = mapped_column(Float)
    bedrooms: Mapped[int | None] = mapped_column(Integer)
    bathrooms: Mapped[int | None] = mapped_column(Integer)

    origin_channel: Mapped[str | None] = mapped_column(String(50), index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )

    asset = relationship("Asset", back_populates="listings")
    contact = relationship("Contact", back_populates="listings")
    market_events = relationship("MarketEvent", back_populates="listing")
    snapshots = relationship("ListingSnapshot", back_populates="listing")