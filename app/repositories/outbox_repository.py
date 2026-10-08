from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox import OutboxEvent, OutboxStatus


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def add(self, event: OutboxEvent) -> OutboxEvent:
        self.session.add(event)
        await self.session.flush()
        return event

    async def get_pending_batch(self, limit: int) -> list[OutboxEvent]:
        stmt: Select[tuple[OutboxEvent]] = (
            select(OutboxEvent)
            .where(OutboxEvent.status == OutboxStatus.pending)
            .order_by(OutboxEvent.created_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def mark_published(self, event: OutboxEvent) -> None:
        event.status = OutboxStatus.published
        event.published_at = datetime.now(UTC)
        event.attempts += 1
        event.last_error = None
        await self.session.flush()

    async def mark_failed(self, event: OutboxEvent, error: str) -> None:
        event.status = OutboxStatus.pending
        event.last_error = error
        event.attempts += 1
        await self.session.flush()
