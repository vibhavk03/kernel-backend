from sqlalchemy import Integer, DateTime, ForeignKey, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.session import Base


class RawRow(Base):
    __tablename__ = "raw_rows"

    id: Mapped[int] = mapped_column(primary_key=True)

    ingestion_id: Mapped[int] = mapped_column(
        ForeignKey("raw_ingestions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    row_index: Mapped[int] = mapped_column(Integer, nullable=False)
    data: Mapped[dict] = mapped_column(JSONB, nullable=False)

    inserted_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    ingestion = relationship("RawIngestion", back_populates="rows")