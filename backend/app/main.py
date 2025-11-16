# backend/app/main.py
import os
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Routers
from app.api import upload  # Your upload endpoints

# Database
from app.db import engine
from app.api.models import Base


logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger("uvicorn")


def create_app() -> FastAPI:
    app = FastAPI(
        title="NIRF Budget Analysis API",
        version="1.0",
        description="API for PDF upload, extraction, Supabase upload, and DB storage"
    )

    # -------------------------
    # CORS (allow local testing)
    # -------------------------
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],     # allow all during testing
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # -------------------------
    #Routers
    app.include_router(upload.router, prefix="/api", tags=["upload"])
    from app.api import capital_predict
    app.include_router(capital_predict.router, prefix="/api", tags=["prediction"])
    from app.api import feature_imp
    app.include_router(feature_imp.router, prefix="/api", tags=["model"])
    from app.api import storage
    app.include_router(storage.router, prefix="/api", tags=["storage"])


    # -------------------------
    # Health Check
    # -------------------------
    @app.get("/health")
    def health():
        return {"status": "ok"}

    return app


app = create_app()


# ---------------------------------------------------
# Start-up: create tables (ONLY in dev mode)
# ---------------------------------------------------
@app.on_event("startup")
def startup_event():
    LOG.info("🚀 Starting API...")
    try:
        Base.metadata.create_all(bind=engine)
        LOG.info("✅ Database tables ensured")
    except Exception as e:
        LOG.error(f"❌ Error creating tables: {e}")


@app.get("/", include_in_schema=False)
def home():
    return {"message": "API running. Go to /docs."}
