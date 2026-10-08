from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from faststream.rabbit import TestRabbitBroker as RabbitTestBroker

from app.models.payment import PaymentStatus
from app.schemas.events import PaymentCreatedEvent
from app.workers import consumer


@pytest.fixture
def processing(monkeypatch, event_data):
    event = PaymentCreatedEvent(**event_data)
    payment = SimpleNamespace(
        id=event.payment_id, status=PaymentStatus.pending, webhook_sent_at=None
    )
    repository = SimpleNamespace(
        get_by_id=AsyncMock(return_value=payment),
        update_status=AsyncMock(
            side_effect=lambda target, status: setattr(target, "status", status)
        ),
        mark_webhook_sent=AsyncMock(
            side_effect=lambda target: setattr(target, "webhook_sent_at", datetime.now(UTC))
        ),
    )
    session = AsyncMock()
    session.__aenter__.return_value = session
    monkeypatch.setattr(consumer, "PaymentRepository", lambda _: repository)
    monkeypatch.setattr(consumer, "async_session_maker", lambda: session)
    gateway = Mock(return_value=0)
    monkeypatch.setattr(consumer.random, "random", gateway)
    monkeypatch.setattr(consumer.random, "randint", Mock(return_value=0))
    monkeypatch.setattr(consumer.asyncio, "sleep", AsyncMock())
    webhook = AsyncMock()
    monkeypatch.setattr(consumer, "send_payment_webhook", webhook)
    message = SimpleNamespace(reject=AsyncMock())
    return SimpleNamespace(
        event=event,
        payment=payment,
        session=session,
        repository=repository,
        gateway=gateway,
        webhook=webhook,
        message=message,
    )


async def test_subscriber_receives_outbox_payload(monkeypatch, event_data):
    process = AsyncMock()
    session = AsyncMock()
    monkeypatch.setattr(consumer, "_process_payment", process)
    monkeypatch.setattr(consumer, "async_session_maker", lambda: session)
    async with RabbitTestBroker(consumer.broker):
        await consumer.broker.publish(
            event_data,
            exchange=consumer.payments_exchange,
            routing_key=consumer.settings.payments_routing_key,
        )
    assert process.await_count == 1
    assert process.await_args.args[1].payment_id == PaymentCreatedEvent(**event_data).payment_id


@pytest.mark.parametrize("failures", [1, 2])
async def test_retry_webhook_without_repeating_payment(processing, failures):
    processing.webhook.side_effect = [RuntimeError("temporary failure")] * failures + [None]
    await consumer.consume_payment(processing.event, processing.message)
    assert processing.webhook.await_count == failures + 1
    assert processing.gateway.call_count == 1
    assert processing.payment.status == PaymentStatus.succeeded
    assert processing.payment.webhook_sent_at is not None
    processing.message.reject.assert_not_awaited()


async def test_reject_after_three_failed_webhooks(processing):
    processing.webhook.side_effect = RuntimeError("unavailable")
    await consumer.consume_payment(processing.event, processing.message)
    assert processing.webhook.await_count == 3
    assert processing.gateway.call_count == 1
    assert processing.payment.webhook_sent_at is None
    processing.message.reject.assert_awaited_once_with(requeue=False)
    assert [call.args[0] for call in consumer.asyncio.sleep.await_args_list] == [0, 1, 2]


async def test_redelivery_does_not_repeat_completed_work(processing):
    await consumer._process_payment(processing.session, processing.event)
    await consumer._process_payment(processing.session, processing.event)
    assert processing.gateway.call_count == 1
    assert processing.webhook.await_count == 1


async def test_failed_payment_also_sends_webhook(processing):
    processing.gateway.return_value = 1
    await consumer._process_payment(processing.session, processing.event)
    assert processing.payment.status == PaymentStatus.failed
    processing.webhook.assert_awaited_once()


async def test_missing_payment_is_not_silently_acknowledged(processing):
    processing.repository.get_by_id.return_value = None
    await consumer.consume_payment(processing.event, processing.message)
    assert processing.repository.get_by_id.await_count == 3
    processing.message.reject.assert_awaited_once_with(requeue=False)
    processing.webhook.assert_not_awaited()
