# from sqlalchemy.orm import Session
# from pathlib import Path
# import pandas as pd
# import math
# from datetime import datetime, date
import pandas as pd
from pathlib import Path
from sqlalchemy.orm import Session
from app.repositories.ingestion_repo import IngestionRepository

class IngestionService:
    
    @staticmethod
    def process_local_file(db: Session):
        # --- 1. EXTRACT ---
        # Point to the file in your data_local folder
        base_dir = Path(__file__).resolve().parent.parent
        file_path = base_dir / "data_local" / "IQVIA" / "IQVIA_OneKey_Affiliations.csv"
        
        if not file_path.exists():
            raise FileNotFoundError(f"Could not find file at {file_path}")
            
        df = pd.read_csv(file_path)
        
        # --- 2. TRANSFORM ---
        # Your toy transform
        df['processed_by'] = 'fastapi_service_layer'
        # Drop empty columns, rename things, etc.
        
        # --- 3. LOAD (Pass to Repository) ---
        # We let the repository handle the actual database insertion
        rows_inserted = IngestionRepository.save_dataframe(db, df, "raw_iqvia_affiliations")
        
        return {"message": "ETL complete", "rows": rows_inserted}

# from app.repositories.ingestion_repo import (
#     create_ingestion,
#     bulk_insert_rows,
#     mark_ingestion_success,
#     mark_ingestion_failed,
# )


# def _normalize_col(name: str) -> str:
#     # simple normalization; you can improve later
#     s = str(name).strip().lower()
#     s = "".join(ch if ch.isalnum() else "_" for ch in s)
#     while "__" in s:
#         s = s.replace("__", "_")
#     s = s.strip("_")
#     return s or "col"


# def _make_unique_cols(cols: list[str]) -> list[str]:
#     seen = {}
#     out = []
#     for c in cols:
#         base = _normalize_col(c)
#         if base not in seen:
#             seen[base] = 1
#             out.append(base)
#         else:
#             seen[base] += 1
#             out.append(f"{base}_{seen[base]}")
#     return out


# def _json_safe(v):
#     # Convert pandas/py values into JSON-safe values
#     if v is None:
#         return None
#     if isinstance(v, float) and math.isnan(v):
#         return None
#     if isinstance(v, (datetime, date)):
#         return v.isoformat()
#     # pandas timestamp
#     if hasattr(v, "to_pydatetime"):
#         return v.to_pydatetime().isoformat()
#     return v


# def ingest_excel_to_raw(db: Session, *, vendor: str, dataset: str, file_path: str,
#                         sheet_name: str | None, schema_version: str | None,
#                         limit_rows: int | None) -> tuple[int, int, str, str]:
#     """
#     Returns: (ingestion_id, rows_inserted, status, file_name)
#     """
#     p = Path(file_path)
#     if not p.exists():
#         raise FileNotFoundError(f"File not found: {file_path}")
#     if p.suffix.lower() not in [".xlsx", ".xlsm", ".xls"]:
#         raise ValueError("Only Excel files (.xlsx/.xlsm/.xls) are supported")

#     ingestion = create_ingestion(
#         db,
#         vendor=vendor,
#         dataset=dataset,
#         source_path=str(p),
#         file_name=p.name,
#         sheet_name=sheet_name,
#         schema_version=schema_version,
#     )
#     ingestion_id = ingestion.id

#     try:
#         df = pd.read_excel(p, sheet_name=sheet_name)  # openpyxl handles xlsx
#         if limit_rows:
#             df = df.head(limit_rows)

#         # normalize columns
#         df.columns = _make_unique_cols(list(df.columns))

#         # convert to dicts
#         records = df.to_dict(orient="records")

#         # build row payloads
#         batch = []
#         inserted = 0
#         BATCH_SIZE = 5000

#         for idx, row in enumerate(records, start=1):
#             row_safe = {k: _json_safe(v) for k, v in row.items()}
#             batch.append({"row_index": idx, "data": row_safe})

#             if len(batch) >= BATCH_SIZE:
#                 inserted += bulk_insert_rows(db, ingestion_id=ingestion_id, rows=batch)
#                 batch.clear()

#         if batch:
#             inserted += bulk_insert_rows(db, ingestion_id=ingestion_id, rows=batch)

#         mark_ingestion_success(db, ingestion_id=ingestion_id, row_count=inserted)
#         db.commit()
#         return ingestion_id, inserted, "SUCCESS", p.name

#     except Exception as e:
#         db.rollback()
#         # mark failed (separate txn)
#         try:
#             mark_ingestion_failed(db, ingestion_id=ingestion_id, error_message=str(e))
#             db.commit()
#         except Exception:
#             db.rollback()
#         raise