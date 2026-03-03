from sqlalchemy.orm import Session
from sqlalchemy import update

from app.models.raw_ingestion import RawIngestion
from app.models.raw_row import RawRow

import pandas as pd
from sqlalchemy.orm import Session

class IngestionRepository:
    
    @staticmethod
    def save_dataframe(db: Session, df: pd.DataFrame, table_name: str):
        # Get the underlying SQLAlchemy engine from the session
        engine = db.get_bind()
        
        # Write the dataframe to the database
        # Using 'append' so it doesn't drop the table if it exists
        df.to_sql(table_name, engine, if_exists='append', index=False)
        
        return len(df)


# def create_ingestion(
#     db: Session,
#     *,
#     vendor: str,
#     dataset: str,
#     source_path: str,
#     file_name: str,
#     sheet_name: str | None,
#     schema_version: str | None,
# ) -> RawIngestion:
#     ingestion = RawIngestion(
#         vendor=vendor,
#         dataset=dataset,
#         source_path=source_path,
#         file_name=file_name,
#         sheet_name=sheet_name,
#         schema_version=schema_version,
#         status="PROCESSING",
#     )
#     db.add(ingestion)
#     db.flush()  # assigns ingestion.id without committing
#     return ingestion


# def bulk_insert_rows(db: Session, *, ingestion_id: int, rows: list[dict]) -> int:
#     """
#     rows = [{"row_index": 1, "data": {...}}, ...]
#     """
#     if not rows:
#         return 0

#     objs = [
#         RawRow(ingestion_id=ingestion_id, row_index=r["row_index"], data=r["data"])
#         for r in rows
#     ]
#     db.add_all(objs)
#     return len(objs)


# def mark_ingestion_success(db: Session, *, ingestion_id: int, row_count: int) -> None:
#     db.execute(
#         update(RawIngestion)
#         .where(RawIngestion.id == ingestion_id)
#         .values(status="SUCCESS", row_count=row_count, finished_at=func.now())
#     )


# def mark_ingestion_failed(db: Session, *, ingestion_id: int, error_message: str) -> None:
#     db.execute(
#         update(RawIngestion)
#         .where(RawIngestion.id == ingestion_id)
#         .values(status="FAILED", error_message=error_message, finished_at=func.now())
#     )