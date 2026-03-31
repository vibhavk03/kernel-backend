import pandas as pd
from sqlalchemy.orm import Session
from app.repositories.ingestion_repo import IngestionRepository
from app.repositories.ingestion_repo import IngestionRepository
from app.repositories.ingestion_tracking_repo import IngestionTrackingRepository
from app.repositories.provider_analytics_repo import ProviderAnalyticsRepository
from app.services.s3_service import S3Service
from app.core.ingestion_config import INGESTION_FILE_CONFIGS
from app.core.config import settings
from app.utils.build_analytics_table import build_analytics_table


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

            df = pd.read_csv(
                s3_service.get_file_bytes(s3_key),
                dtype=dtype_map,
            )

            df = transformer.transform(df)
            validator.validate(df)

            processed_files[source_name] = {
                "key": s3_key,
                "target_table": target_table,
                "df": df,
                "status": "validated",
                "rows_loaded": None,
            }

        # --- 2. CROSS-FILE TRANSFORM LOGIC ---
        # Barbie & Grace's transformation logic would go here to create our new table in db
        # Toy transform
        # would need idempotency logic here as well, will implment it after we finalize the transformation logic
        # right now skipping this if all files are already processed
        analytics_df = build_analytics_table(processed_files)
        analytics_df["processed_by"] = "fastapi_service_layer"

        # --- 3. LOAD TO DB ---
        table_name = "provider_analytics"
        rows_loaded = ProviderAnalyticsRepository.save_dataframe(
            db,
            analytics_df,
            table_name,
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
