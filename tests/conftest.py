import os
from uuid import uuid4

import pytest


def pytest_collection_modifyitems(config, items):
    if os.getenv("RUN_INTEGRATION_TESTS") == "1":
        return
    skip = pytest.mark.skip(reason="Set RUN_INTEGRATION_TESTS=1 to use PostgreSQL and RabbitMQ")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


@pytest.fixture
def payload():
    return {
        "amount": "1500.50",
        "currency": "RUB",
        "description": "Test payment",
        "metadata": {"order_id": "ORD-1"},
        "webhook_url": "https://example.com/webhook",
    }


@pytest.fixture
def event_data(payload):
    return {
        **payload,
        "payment_id": str(uuid4()),
        "status": "pending",
        "idempotency_key": str(uuid4()),
    }
