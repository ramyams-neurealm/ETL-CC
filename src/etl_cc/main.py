"""FastAPI application entry point for ETL Migration Command Center."""

from fastapi import FastAPI

from etl_cc.api import router
from etl_cc.config import settings


app = FastAPI(
    title=settings.app_name,
    version="1.0.0",
)


app.include_router(
    router,
    prefix=settings.api_prefix,
)


@app.get("/", tags=["Application"])
async def root() -> dict[str, str]:
    return {
        "application": settings.app_name,
        "status": "running",
        "docs": "/docs",
    }
