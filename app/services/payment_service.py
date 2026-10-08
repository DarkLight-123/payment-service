from __future__ import annotations

from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox import OutboxEvent
from app.models.payment import Payment
from app.repositories.outbox_repository import OutboxRepository
from app.repositories.payment_repository import PaymentRepository
from app.schemas.payment import PaymentCreateRequest


class PaymentService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.payment_repository = PaymentRepository(session)
        self.outbox_repository = OutboxRepository(session)

    async def create_payment(
        self,
        payload: PaymentCreateRequest,
        idempotency_key: str,
    ) -> Payment:
        existing_payment = await self.payment_repository.get_by_idempotency_key(idempotency_key)
        if existing_payment is not None:
            return self._check_repeated_request(existing_payment, payload)

        payment = Payment(
            amount=payload.amount,
            currency=payload.currency,
            description=payload.description,
            metadata_json=payload.metadata,
            idempotency_key=idempotency_key,
            webhook_url=str(payload.webhook_url),
        )
        try:
            await self.payment_repository.add(payment)
            outbox_event = OutboxEvent(
                aggregate_type="payment",
                aggregate_id=payment.id,
                event_type="payment.created",
                payload={
                    "payment_id": str(payment.id),
                    "amount": str(payment.amount),
                    "currency": payment.currency,
                    "description": payment.description,
                    "metadata": payment.metadata_json,
                    "webhook_url": payment.webhook_url,
                    "status": payment.status.value,
                    "idempotency_key": payment.idempotency_key,
                },
            )
            await self.outbox_repository.add(outbox_event)
            await self.session.commit()
        except IntegrityError:
            await self.session.rollback()
            existing_payment = await self.payment_repository.get_by_idempotency_key(idempotency_key)
            if existing_payment is None:
                raise
            return self._check_repeated_request(existing_payment, payload)
        await self.session.refresh(payment)
        return payment

    @staticmethod
    def _check_repeated_request(payment: Payment, payload: PaymentCreateRequest) -> Payment:
        if (
            payment.amount != payload.amount
            or payment.currency != payload.currency
            or payment.description != payload.description
            or payment.metadata_json != payload.metadata
            or payment.webhook_url != str(payload.webhook_url)
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Idempotency-Key was already used with a different request",
            )
        return payment

    async def get_payment(self, payment_id: UUID) -> Payment:
        payment = await self.payment_repository.get_by_id(payment_id)
        if payment is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Payment not found",
            )
        return payment
