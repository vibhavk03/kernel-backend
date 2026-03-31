import pandas as pd
from sqlalchemy.orm import Session
from app.models.ingestion_run import IngestionRunType
from app.repositories.ingestion_tracking_repo import IngestionTrackingRepository
from app.repositories.provider_analytics_repo import ProviderAnalyticsRepository
from app.services.s3_service import S3Service
from app.core.ingestion_config import INGESTION_FILE_CONFIGS
from app.core.config import settings
from app.utils.build_analytics_table import build_analytics_table
from app.utils.build_final_signature import build_final_signature


class IngestionService:

    @staticmethod
    def process_files(db: Session):
        # object to store the processed results for each file
        processed_files = {}
        s3_service = S3Service()

        # --- 1. EXTRACT + TRANSFORM + VALIDATE ---
        for source_name, config in INGESTION_FILE_CONFIGS.items():
            s3_key = config["key"]
            dtype_map = config["dtype"]
            transformer = config["transformer"]
            validator = config["validator"]
            target_table = config["target_table"]

            file_bytes, metadata = s3_service.get_file_with_metadata(s3_key)

            df = pd.read_csv(
                file_bytes,
                dtype=dtype_map,
            )

            df = transformer.transform(df)
            validator.validate(df)

            processed_files[source_name] = {
                "key": s3_key,
                "etag": metadata["etag"],
                "last_modified": metadata["last_modified"],
                "target_table": target_table,
                "df": df,
                "status": "validated",
            }

        # --- 2. CROSS-FILE TRANSFORM LOGIC ---
        # Barbie & Grace's transformation logic would go here to create our new table in db
        # Toy transform
        analytics_df = build_analytics_table(processed_files)
        analytics_df["processed_by"] = "fastapi_service_layer"

        table_name = "provider_analytics"
        final_signature = build_final_signature(processed_files)

        # --- 3. IDEMPOTENCY CHECK ---
        if settings.IDEMPOTENCY_ENABLED:
            already_processed = IngestionTrackingRepository.already_processed(
                db=db,
                run_type=IngestionRunType.DERIVED_TABLE,
                s3_key=table_name,
                etag=final_signature,
                target_table=table_name,
            )

            if already_processed:
                # adding ingestion run record with status skipped here for tracking purposes
                IngestionTrackingRepository.create_record(
                    db=db,
                    run_type=IngestionRunType.DERIVED_TABLE,
                    source_name=table_name,
                    s3_key=table_name,
                    etag=final_signature,
                    target_table=table_name,
                    status="skipped",
                    rows_loaded=0,
                )

                return {
                    "message": "ETL skipped - provider analytics already up to date",
                    "idempotency_enabled": True,
                    "results": {
                        "target_table": table_name,
                        "status": "skipped",
                        "rows_loaded": 0,
                    },
                }

        # --- 4. LOAD TO DB ---

        # if not already processed, or not up to date, we first clear the table
        ProviderAnalyticsRepository.clear_table(db, table_name)

        rows_loaded = ProviderAnalyticsRepository.save_dataframe(
            db,
            analytics_df,
            table_name,
        )

        # adding ingestion run record with final signature
        IngestionTrackingRepository.create_record(
            db=db,
            run_type=IngestionRunType.DERIVED_TABLE,
            source_name=table_name,
            s3_key=table_name,
            etag=final_signature,
            target_table=table_name,
            status="success",
            rows_loaded=rows_loaded,
        )

        # --- 4. RESPONSE SUMMARY ---
        return {
            "message": "ETL complete",
            "results": {
                "analytics_360": {
                    "target_table": table_name,
                    "status": "loaded",
                    "rows_loaded": rows_loaded,
                }
            },
        }
