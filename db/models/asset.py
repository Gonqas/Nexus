from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base, utc_now


class Asset(Base):
    __tablename__ = "assets"

    id: Mapped[int] = mapped_column(primary_key=True)
    building_id: Mapped[int | None] = mapped_column(ForeignKey("buildings.id"), index=True)

    asset_type_family: Mapped[str | None] = mapped_column(String(100), index=True)
    asset_type_detail: Mapped[str | None] = mapped_column(String(100), index=True)

    address_raw: Mapped[str | None] = mapped_column(String(255))
    address_norm: Mapped[str | None] = mapped_column(String(255), index=True)

    district: Mapped[str | None] = mapped_column(String(100), index=True)
    neighborhood: Mapped[str | None] = mapped_column(String(100), index=True)

    lat: Mapped[float | None] = mapped_column(Float)
    lon: Mapped[float | None] = mapped_column(Float)
    cadastral_ref: Mapped[str | None] = mapped_column(String(50), index=True)

    area_m2: Mapped[float | None] = mapped_column(Float)
    bedrooms: Mapped[int | None] = mapped_column(Integer)
    bathrooms: Mapped[int | None] = mapped_column(Integer)
    year_built: Mapped[int | None] = mapped_column(Integer)

    data_confidence: Mapped[float | None] = mapped_column(Float)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )

    building = relationship("Building", back_populates="assets")
    listings = relationship("Listing", back_populates="asset")
    market_events = relationship("MarketEvent", back_populates="asset")