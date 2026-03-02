from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.ingestion import ExcelIngestRequest, ExcelIngestResponse
from app.services.ingestion_service import ingest_excel_to_raw

router = APIRouter(prefix="/ingest", tags=["ingest"])

@router.post("/excel", response_model=ExcelIngestResponse)

def ingest_excel(req: ExcelIngestRequest, db: Session = Depends(get_db)):
    ingestion_id, rows_inserted, status, file_name = ingest_excel_to_raw(
        db,
        vendor=req.vendor,
        dataset=req.dataset,
        file_path=req.file_path,
        sheet_name=req.sheet_name,
        schema_version=req.schema_version,
        limit_rows=req.limit_rows,
    )
    return ExcelIngestResponse(
        ingestion_id=ingestion_id,
        rows_inserted=rows_inserted,
        status=status,
        file_name=file_name,
    )