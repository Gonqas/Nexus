from datetime import datetime

from sqlalchemy import DateTime, Float, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base, utc_now


class Contact(Base):
    __tablename__ = "contacts"

    id: Mapped[int] = mapped_column(primary_key=True)

    phone_raw: Mapped[str | None] = mapped_column(String(50))
    phone_norm: Mapped[str | None] = mapped_column(String(50), index=True)

    name_raw: Mapped[str | None] = mapped_column(String(255))
    name_norm: Mapped[str | None] = mapped_column(String(255), index=True)

    contact_type_guess: Mapped[str | None] = mapped_column(String(50))
    owner_listing_count_latest: Mapped[int | None] = mapped_column(Integer)
    recurrence_score: Mapped[float | None] = mapped_column(Float)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )

    listings = relationship("Listing", back_populates="contact")