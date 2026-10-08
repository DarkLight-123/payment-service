from __future__ import annotations

import asyncio
import logging
from contextlib import suppress

from app.broker import broker, payments_exchange
from app.core.config import settings
from app.db.session import async_session_maker
from app.repositories.outbox_repository import OutboxRepository

logger = logging.getLogger(__name__)


class OutboxPublisher:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="outbox-publisher")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def publish_batch(self) -> int:
        async with async_session_maker() as session, session.begin():
            repository = OutboxRepository(session)
            events = await repository.get_pending_batch(settings.outbox_batch_size)
            for event in events:
                try:
                    await broker.publish(
                        event.payload,
                        exchange=payments_exchange,
                        routing_key=settings.payments_routing_key,
                        persist=True,
                        mandatory=True,
                        timeout=10,
                        message_id=str(event.id),
                    )
                except Exception as exc:
                    await repository.mark_failed(event, str(exc))
                    logger.exception("Failed to publish outbox event %s", event.id)
                else:
                    await repository.mark_published(event)
            return len(events)

    async def _run(self) -> None:
        while True:
            try:
                await self.publish_batch()
            except Exception:
                logger.exception("Outbox publisher failed")
            await asyncio.sleep(settings.outbox_poll_interval_seconds)
