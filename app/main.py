from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from fastapi.openapi.docs import get_swagger_ui_html

from app.api.dependencies import verify_api_key
from app.api.router import api_router
from app.broker import broker, declare_payment_topology
from app.core.config import settings
from app.db.session import engine
from app.workers.outbox_publisher import OutboxPublisher

publisher = OutboxPublisher()


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        await broker.connect()
        await declare_payment_topology()
        await publisher.start()
        yield
    finally:
        await publisher.stop()
        try:
            await broker.stop()
        finally:
            await engine.dispose()


app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
    dependencies=[Depends(verify_api_key)],
)

app.include_router(api_router, prefix=settings.api_v1_prefix)


@app.get("/health", tags=["health"])
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/openapi.json", include_in_schema=False)
async def openapi_schema():
    return app.openapi()


@app.get("/docs", include_in_schema=False)
async def swagger_ui():
    return get_swagger_ui_html(openapi_url="/openapi.json", title=settings.app_name)
