from fastapi import FastAPI

from app.api.v1.router import router as v1_router
from app.db.session import Base, engine

# IMPORTANT: for local dev only.
# In production you'd use Alembic migrations instead of create_all.
Base.metadata.create_all(bind=engine)


def create_app() -> FastAPI:
    app = FastAPI(title="Kernel Backend API")

    app.include_router(v1_router, prefix="/api/v1")

    @app.get("/", tags=["root"])
    def root():
        return {"message": "Welcome to the Kernel Backend API"}

    return app


app = create_app()
