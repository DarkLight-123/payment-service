from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from fastapi import HTTPException

from app.api.dependencies import get_db_session
from app.core.config import settings
from app.main import app
from app.services.payment_service import PaymentService


@pytest.fixture
async def client():
    async def session_override():
        yield AsyncMock()

    app.dependency_overrides[get_db_session] = session_override
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client
    app.dependency_overrides.clear()


@pytest.mark.parametrize("key", [None, "wrong-key"])
@pytest.mark.parametrize(
    "method,path",
    [
        ("GET", "/health"),
        ("GET", "/docs"),
        ("GET", "/openapi.json"),
        ("GET", f"/api/v1/payments/{uuid4()}"),
        ("POST", "/api/v1/payments"),
    ],
)
async def test_api_key_required(client, key, method, path, payload):
    headers = {"Idempotency-Key": "test"}
    if key is not None:
        headers["X-API-Key"] = key
    response = await client.request(method, path, headers=headers, json=payload)
    assert response.status_code == 401


async def test_health(client):
    response = await client.get("/health", headers={"X-API-Key": settings.api_key})
    assert response.status_code == 200


@pytest.mark.parametrize("key", [None, "", " ", "a" * 129])
async def test_invalid_idempotency_key(client, payload, key):
    headers = {"X-API-Key": settings.api_key}
    if key is not None:
        headers["Idempotency-Key"] = key
    response = await client.post("/api/v1/payments", headers=headers, json=payload)
    assert response.status_code == 422


@pytest.mark.parametrize(
    "changes",
    [
        {"amount": "0"},
        {"amount": "-10"},
        {"amount": "1.001"},
        {"amount": "10000000000000000"},
        {"currency": "GBP"},
        {"description": ""},
        {"webhook_url": "not-a-url"},
        {"webhook_url": "https://example.com/" + "a" * 1024},
        {"unexpected": True},
    ],
)
async def test_invalid_payment(client, payload, changes):
    response = await client.post(
        "/api/v1/payments",
        headers={"X-API-Key": settings.api_key, "Idempotency-Key": "test"},
        json={**payload, **changes},
    )
    assert response.status_code == 422


async def test_missing_payment(client, monkeypatch):
    monkeypatch.setattr(
        PaymentService,
        "get_payment",
        AsyncMock(side_effect=HTTPException(404, "Payment not found")),
    )
    response = await client.get(
        f"/api/v1/payments/{uuid4()}", headers={"X-API-Key": settings.api_key}
    )
    assert response.status_code == 404
