"""FastAPI application entry point for ETL Migration Command Center."""

from fastapi import FastAPI, Request
import time
from fastapi.middleware.cors import CORSMiddleware

from etl_cc.api import router
from etl_cc.config import settings
from etl_cc.logging_config import configure_logging, log_event, log_exception


app = FastAPI(
    title=settings.app_name,
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

logger = configure_logging("API")
@app.middleware("http")
async def operational_logging(request: Request, call_next):
    started=time.perf_counter();log_event(logger,"HTTP_REQUEST_STARTED",method=request.method,path=request.url.path,query=dict(request.query_params))
    try:
        response=await call_next(request);log_event(logger,"HTTP_REQUEST_COMPLETED",method=request.method,path=request.url.path,status_code=response.status_code,duration_ms=round((time.perf_counter()-started)*1000,2));return response
    except Exception as exc:
        log_exception(logger,"HTTP_REQUEST_FAILED",exc,method=request.method,path=request.url.path);raise


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
