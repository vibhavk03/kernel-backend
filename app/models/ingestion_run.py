import enum

from sqlalchemy import Column, Integer, String, DateTime, Text, Enum
from sqlalchemy.sql import func

from app.db.session import Base


class IngestionRunType(str, enum.Enum):
    SOURCE_FILE = "source_file"
    DERIVED_TABLE = "derived_table"


class IngestionRun(Base):
    __tablename__ = "ingestion_runs"

    id = Column(Integer, primary_key=True, index=True)

    run_type = Column(
        Enum(IngestionRunType, name="ingestion_run_type"),
        nullable=False,
        index=True,
    )
    source_name = Column(String, nullable=False, index=True)
    s3_key = Column(String, nullable=False, index=True)
    etag = Column(String, nullable=False, index=True)
    target_table = Column(String, nullable=False, index=True)

    status = Column(
        String, nullable=False, index=True
    )  # processing, success, failed, skipped
    rows_loaded = Column(Integer, nullable=True)
    error_message = Column(Text, nullable=True)

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    completed_at = Column(DateTime(timezone=True), nullable=True)
