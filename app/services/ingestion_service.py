import pandas as pd
from sqlalchemy.orm import Session
from app.repositories.ingestion_repo import IngestionRepository
from app.repositories.ingestion_repo import IngestionRepository
from app.services.s3_service import S3Service
from app.core.ingestion_config import INGESTION_FILE_CONFIGS


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
        for source_name, file_info in processed_files.items():
            df = file_info["df"]
            df["processed_by"] = "fastapi_service_layer"
            file_info["df"] = df

        # --- 3. LOAD TO DB ---
        for source_name, file_info in processed_files.items():
            rows_loaded = IngestionRepository.save_dataframe(
                db,
                file_info["df"],
                file_info["target_table"],
            )

            file_info["rows_loaded"] = rows_loaded
            file_info["status"] = "loaded"

        # --- 4. RESPONSE SUMMARY ---
        results = {}
        for source_name, file_info in processed_files.items():
            results[source_name] = {
                "key": file_info["key"],
                "target_table": file_info["target_table"],
                "status": file_info["status"],
                "rows_loaded": file_info["rows_loaded"],
            }

        return {
            "message": "ETL complete",
            "results": results,
        }
