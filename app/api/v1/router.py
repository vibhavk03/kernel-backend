from fastapi import APIRouter

from app.api.v1.endpoints.ingestion import router as ingest_router

router = APIRouter()
router.include_router(ingest_router)