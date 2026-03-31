from sqlalchemy.orm import Session

from app.models.ingestion_run import IngestionRun, IngestionRunType


class IngestionTrackingRepository:

    @staticmethod
    def already_processed(
        db: Session,
        run_type: IngestionRunType,
        s3_key: str,
        etag: str,
        target_table: str,
    ) -> bool:
        record = (
            db.query(IngestionRun)
            .filter(
                IngestionRun.run_type == run_type,
                IngestionRun.s3_key == s3_key,
                IngestionRun.etag == etag,
                IngestionRun.target_table == target_table,
                IngestionRun.status == "success",
            )
            .first()
        )
        return record is not None

    @staticmethod
    def create_record(
        db: Session,
        run_type: IngestionRunType,
        source_name: str,
        s3_key: str,
        etag: str,
        target_table: str,
        status: str,
        rows_loaded: int | None = None,
        error_message: str | None = None,
    ) -> IngestionRun:
        record = IngestionRun(
            run_type=run_type,
            source_name=source_name,
            s3_key=s3_key,
            etag=etag,
            target_table=target_table,
            status=status,
            rows_loaded=rows_loaded,
            error_message=error_message,
        )
        db.add(record)
        db.commit()
        db.refresh(record)
        return record
