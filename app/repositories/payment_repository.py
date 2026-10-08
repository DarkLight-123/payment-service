from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.payment import Payment, PaymentStatus


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_by_id(self, payment_id: UUID, *, for_update: bool = False) -> Payment | None:
        query = select(Payment).where(Payment.id == payment_id)
        if for_update:
            query = query.with_for_update().execution_options(populate_existing=True)
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        result = await self.session.execute(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        return result.scalar_one_or_none()

    async def add(self, payment: Payment) -> Payment:
        self.session.add(payment)
        await self.session.flush()
        return payment

    async def update_status(self, payment: Payment, status: PaymentStatus) -> Payment:
        payment.status = status
        payment.processed_at = datetime.now(UTC)
        await self.session.flush()
        return payment

    async def mark_webhook_sent(self, payment: Payment) -> None:
        payment.webhook_sent_at = datetime.now(UTC)
        await self.session.flush()
