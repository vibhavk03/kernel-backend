from sqlalchemy import String, Text, Integer, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.session import Base


class RawIngestion(Base):
    __tablename__ = "raw_ingestions"

    id: Mapped[int] = mapped_column(primary_key=True)

    vendor: Mapped[str] = mapped_column(String(100), nullable=False)
    dataset: Mapped[str] = mapped_column(String(150), nullable=False)

    source_path: Mapped[str] = mapped_column(Text, nullable=False)
    file_name: Mapped[str] = mapped_column(String(255), nullable=False)

    sheet_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    schema_version: Mapped[str | None] = mapped_column(String(50), nullable=True)

    status: Mapped[str] = mapped_column(String(20), nullable=False, default="PROCESSING")
    row_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    started_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    rows = relationship("RawRow", back_populates="ingestion", cascade="all, delete-orphan")