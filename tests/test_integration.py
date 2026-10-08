import asyncio
import json
import os
import subprocess
import sys
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import aio_pika
import httpx
import pytest
from sqlalchemy import func, make_url, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.api.dependencies import get_db_session
from app.broker import broker, declare_payment_topology
from app.core.config import settings
from app.main import app
from app.models.outbox import OutboxEvent, OutboxStatus
from app.models.payment import Payment
from app.repositories.outbox_repository import OutboxRepository
from app.schemas.payment import PaymentCreateRequest
from app.services.payment_service import PaymentService
from app.workers import consumer, outbox_publisher

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module", autouse=True)
def migrate():
    if os.getenv("RUN_INTEGRATION_TESTS") != "1":
        pytest.skip("Integration tests are disabled")
    if not make_url(settings.effective_database_url).database.endswith("_test"):
        raise RuntimeError("Integration tests require a disposable database ending in _test")
    subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"], check=True)


@pytest.fixture
async def sessions(monkeypatch):
    engine = create_async_engine(settings.effective_database_url, poolclass=NullPool)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE payments, outbox"))
    monkeypatch.setattr(consumer, "async_session_maker", factory)
    monkeypatch.setattr(outbox_publisher, "async_session_maker", factory)
    monkeypatch.setattr(settings, "payment_processing_min_delay_seconds", 0)
    monkeypatch.setattr(settings, "payment_processing_max_delay_seconds", 0)

    async def get_session():
        async with factory() as session:
            yield session

    app.dependency_overrides[get_db_session] = get_session
    yield factory
    app.dependency_overrides.clear()
    await engine.dispose()


@pytest.fixture
async def rabbit(sessions):
    await broker.connect()
    await declare_payment_topology()
    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    channel = await connection.channel()
    queue = await channel.get_queue(settings.payments_queue)
    dlq = await channel.get_queue(settings.payments_dlq)
    await queue.purge()
    await dlq.purge()
    try:
        yield queue, dlq
    finally:
        try:
            await broker.stop()
        finally:
            await connection.close()


async def create_payment(sessions, payload):
    async with sessions() as session:
        return await PaymentService(session).create_payment(
            PaymentCreateRequest(**payload), str(uuid4())
        )


async def wait_for_webhook(sessions, payment_id):
    async with asyncio.timeout(15):
        while True:
            async with sessions() as session:
                payment = await session.get(Payment, payment_id)
                if payment.webhook_sent_at is not None:
                    return payment
            await asyncio.sleep(0.1)


async def test_concurrent_requests_create_one_payment_and_one_event(sessions, payload):
    headers = {"X-API-Key": settings.api_key, "Idempotency-Key": str(uuid4())}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        responses = await asyncio.gather(
            *[client.post("/api/v1/payments", headers=headers, json=payload) for _ in range(20)]
        )
        assert {response.status_code for response in responses} == {202}
        assert len({response.json()["payment_id"] for response in responses}) == 1
        payment_id = responses[0].json()["payment_id"]
        detail = await client.get(f"/api/v1/payments/{payment_id}", headers=headers)
        assert detail.status_code == 200
        assert detail.json()["amount"] == "1500.50"
        assert detail.json()["status"] == "pending"
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 1


async def test_idempotency_key_conflict(sessions, payload):
    headers = {"X-API-Key": settings.api_key, "Idempotency-Key": str(uuid4())}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        first = await client.post("/api/v1/payments", headers=headers, json=payload)
        same = await client.post(
            "/api/v1/payments", headers=headers, json={**payload, "currency": "rub"}
        )
        changed = await client.post(
            "/api/v1/payments", headers=headers, json={**payload, "amount": "99.00"}
        )
        assert first.status_code == same.status_code == 202
        assert first.json()["payment_id"] == same.json()["payment_id"]
        assert changed.status_code == 409


async def test_payment_is_rolled_back_if_outbox_insert_fails(sessions, payload):
    with pytest.raises(RuntimeError):
        async with sessions() as session:
            service = PaymentService(session)
            service.outbox_repository.add = AsyncMock(side_effect=RuntimeError("insert failed"))
            await service.create_payment(PaymentCreateRequest(**payload), str(uuid4()))
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0


async def test_outbox_retries_failed_publication(sessions, payload, monkeypatch):
    await create_payment(sessions, payload)
    publish = AsyncMock(side_effect=[OSError("broker unavailable"), None])
    monkeypatch.setattr(outbox_publisher.broker, "publish", publish)
    publisher = outbox_publisher.OutboxPublisher()
    await publisher.publish_batch()
    async with sessions() as session:
        event = await session.scalar(select(OutboxEvent))
        assert event.status == OutboxStatus.pending
        assert event.attempts == 1
        assert event.last_error == "broker unavailable"
    await publisher.publish_batch()
    async with sessions() as session:
        event = await session.scalar(select(OutboxEvent))
        assert event.status == OutboxStatus.published
        assert event.attempts == 2
        assert event.last_error is None


async def test_outbox_batches_do_not_select_locked_events(sessions, payload):
    await create_payment(sessions, payload)
    async with sessions() as first, sessions() as second:
        assert len(await OutboxRepository(first).get_pending_batch(100)) == 1
        assert await OutboxRepository(second).get_pending_batch(100) == []


async def test_published_messages_are_persistent(sessions, rabbit, payload):
    await create_payment(sessions, payload)
    await outbox_publisher.OutboxPublisher().publish_batch()
    queue, _ = rabbit
    message = await queue.get(timeout=5)
    assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    assert json.loads(message.body)["currency"] == "RUB"
    await message.ack()


async def test_unroutable_event_remains_in_outbox(sessions, rabbit, payload, monkeypatch):
    await create_payment(sessions, payload)
    monkeypatch.setattr(settings, "payments_routing_key", "missing.binding")
    await outbox_publisher.OutboxPublisher().publish_batch()
    async with sessions() as session:
        event = await session.scalar(select(OutboxEvent))
        assert event.status == OutboxStatus.pending
        assert event.last_error is not None


async def test_webhook_retry_and_redelivery(sessions, rabbit, payload, monkeypatch):
    webhook = AsyncMock(side_effect=[RuntimeError("temporary"), None])
    gateway = Mock(return_value=0)
    replay_done = asyncio.Event()
    original_process = consumer._process_payment
    completed = 0

    async def track_process(session, event):
        nonlocal completed
        await original_process(session, event)
        completed += 1
        if completed == 2:
            replay_done.set()

    monkeypatch.setattr(consumer, "send_payment_webhook", webhook)
    monkeypatch.setattr(consumer.random, "random", gateway)
    monkeypatch.setattr(consumer, "_process_payment", track_process)
    payment = await create_payment(sessions, payload)
    await broker.start()
    await outbox_publisher.OutboxPublisher().publish_batch()
    processed = await wait_for_webhook(sessions, payment.id)
    assert processed.status.value in {"succeeded", "failed"}
    assert webhook.await_count == 2
    async with sessions() as session:
        event = await session.scalar(select(OutboxEvent))
        await broker.publish(
            event.payload,
            exchange=consumer.payments_exchange,
            routing_key=settings.payments_routing_key,
            persist=True,
        )
    await asyncio.wait_for(replay_done.wait(), timeout=5)
    assert webhook.await_count == 2
    assert gateway.call_count == 1


async def test_three_failed_webhooks_reach_dlq(sessions, rabbit, payload, monkeypatch):
    webhook = AsyncMock(side_effect=RuntimeError("webhook unavailable"))
    monkeypatch.setattr(consumer, "send_payment_webhook", webhook)
    payment = await create_payment(sessions, payload)
    await broker.start()
    await outbox_publisher.OutboxPublisher().publish_batch()
    _, dlq = rabbit
    message = None
    async with asyncio.timeout(15):
        while message is None:
            message = await dlq.get(fail=False, timeout=1)
            if message is None:
                await asyncio.sleep(0.1)
    assert json.loads(message.body)["payment_id"] == str(payment.id)
    assert webhook.await_count == 3
    async with sessions() as session:
        stored = await session.get(Payment, payment.id)
        assert stored.status.value in {"succeeded", "failed"}
        assert stored.webhook_sent_at is None
    await message.ack()


def test_migrations_can_be_reapplied():
    for revision in ["0001_create_payments_and_outbox", "head"]:
        subprocess.run(
            [
                sys.executable,
                "-m",
                "alembic",
                "upgrade" if revision == "head" else "downgrade",
                revision,
            ],
            check=True,
        )
