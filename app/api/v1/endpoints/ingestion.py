from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.ingestion import ExcelIngestRequest, ExcelIngestResponse
from app.services.ingestion_service import IngestionService

router = APIRouter(prefix="/ingest", tags=["ingest"])


@router.post("/")
def trigger_ingestion(db: Session = Depends(get_db)):
    """
    Endpoint to trigger the ETL pipeline from local storage.
    """
    try:
        # Pass the database session to the service
        result = IngestionService.process_local_file(db)
        return {"status": "success", "details": result}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
