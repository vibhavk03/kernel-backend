# from sqlalchemy.orm import Session
# from pathlib import Path
# import pandas as pd
# import math
# from datetime import datetime, date
import pandas as pd
from pathlib import Path
from sqlalchemy.orm import Session
from app.repositories.ingestion_repo import IngestionRepository
from app.transformers.iqvia_affiliation_transformer import IQVIAAffiliationTransformer
from app.transformers.iqvia_hcp_transformer import IQVIAHCPTransformer
from app.validators.iqvia_affiliation_validator import IQVIAAffiliationValidator
from app.validators.iqvia_hcp_validator import IQVIAHCPValidator


class IngestionService:

    @staticmethod
    def process_local_file(db: Session):
        # --- 1. EXTRACT ---
        base_dir = Path(__file__).resolve().parent.parent
        file_path_affiliation = (
            base_dir / "data_local" / "IQVIA" / "IQVIA_OneKey_Affiliations.csv"
        )
        file_path_hcp = base_dir / "data_local" / "IQVIA" / "IQVIA_OneKey_HCP.csv"

        if not file_path_affiliation.exists():
            raise FileNotFoundError(f"Could not find file at {file_path_affiliation}")

        if not file_path_hcp.exists():
            raise FileNotFoundError(f"Could not find file at {file_path_hcp}")

        # We specify dtypes to ensure consistent reading and to prevent pandas from inferring types that might lead to issues later
        df_affiliation = pd.read_csv(
            file_path_affiliation,
            dtype={
                "onekey_hcp_id": "string",
                "npi": "string",
                "onekey_hco_id": "string",
                "hco_name": "string",
                "affiliation_type": "string",
                "affiliation_status": "string",
            },
        )

        df_hcp = pd.read_csv(
            file_path_hcp,
            dtype={
                "onekey_hcp_id": "string",
                "npi": "string",
                "hcp_name": "string",
                "specialty": "string",
                "status": "string",
                "primary_address_line1": "string",
                "primary_address_line2": "string",
                "primary_city": "string",
                "primary_state": "string",
                "primary_zip": "string",
            },
        )

        # remove rows where every column is empty
        df_affiliation = df_affiliation.dropna(how="all")
        df_hcp = df_hcp.dropna(how="all")

        # --- 2. VALIDATE ---
        df_affiliation = IQVIAAffiliationTransformer.transform(df_affiliation)
        IQVIAAffiliationValidator.validate(df_affiliation)

        df_hcp = IQVIAHCPTransformer.transform(df_hcp)
        IQVIAHCPValidator.validate(df_hcp)

        # --- 3. TRANSFORM ---
        # toy transform
        df_affiliation["processed_by"] = "fastapi_service_layer"
        df_hcp["processed_by"] = "fastapi_service_layer"
        # Drop empty columns, rename things, etc.

        # --- 3. LOAD (Pass to Repository) ---
        # We let the repository handle the actual database insertion
        affiliation_rows = IngestionRepository.save_dataframe(
            db, df_affiliation, "raw_iqvia_affiliations"
        )

        hcp_rows = IngestionRepository.save_dataframe(db, df_hcp, "raw_iqvia_hcps")

        return {
            "message": "ETL complete",
            "affiliation_rows": affiliation_rows,
            "hcp_rows": hcp_rows,
        }


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
