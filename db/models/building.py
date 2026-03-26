from datetime import datetime

from sqlalchemy import DateTime, Float, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base, utc_now


class Building(Base):
    __tablename__ = "buildings"

    id: Mapped[int] = mapped_column(primary_key=True)
    address_base: Mapped[str | None] = mapped_column(String(255), index=True)
    district: Mapped[str | None] = mapped_column(String(100), index=True)
    neighborhood: Mapped[str | None] = mapped_column(String(100), index=True)
    lat: Mapped[float | None] = mapped_column(Float)
    lon: Mapped[float | None] = mapped_column(Float)
    cadastral_ref_base: Mapped[str | None] = mapped_column(String(50), index=True)
    year_built: Mapped[int | None] = mapped_column(Integer)
    protection_level: Mapped[str | None] = mapped_column(String(100))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )

    assets = relationship("Asset", back_populates="building")