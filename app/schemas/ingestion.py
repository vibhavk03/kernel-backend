from pydantic import BaseModel, Field
from typing import Optional


class ExcelIngestRequest(BaseModel):
    vendor: str = Field(..., min_length=1, max_length=100)
    dataset: str = Field(..., min_length=1, max_length=150)
    file_path: str = Field(..., min_length=1)
    sheet_name: Optional[str] = None
    schema_version: Optional[str] = None
    limit_rows: Optional[int] = Field(default=None, gt=0)


class ExcelIngestResponse(BaseModel):
    ingestion_id: int
    rows_inserted: int
    status: str
    file_name: str