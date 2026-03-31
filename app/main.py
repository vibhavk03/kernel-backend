from fastapi import FastAPI
from contextlib import asynccontextmanager

from app.db.session import Base, engine
from app.api.v1.router import router as v1_router
from app.services.scheduler_service import start_scheduler, stop_scheduler

# import models so SQLAlchemy registers them before create_all
from app.models.ingestion_run import IngestionRun
from app.models.provider_analytics import ProviderAnalytics

# IMPORTANT: for local dev only.
# In production you'd use Alembic migrations instead of create_all.
Base.metadata.create_all(bind=engine)


@asynccontextmanager
async def lifespan(app: FastAPI):
    start_scheduler()
    yield
    stop_scheduler()


def create_app() -> FastAPI:
    app = FastAPI(
        title="Kernel Backend API",
        lifespan=lifespan,
    )

    app.include_router(v1_router, prefix="/api/v1")

    @app.get("/", tags=["root"])
    def root():
        return {"message": "Welcome to the Kernel Backend API"}

    return app


app = create_app()
