from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_db_session, verify_api_key
from app.schemas.payment import PaymentCreateRequest, PaymentCreateResponse, PaymentResponse
from app.services.payment_service import PaymentService

router = APIRouter(
    dependencies=[Depends(verify_api_key)],
)


@router.post("", status_code=status.HTTP_202_ACCEPTED, response_model=PaymentCreateResponse)
async def create_payment(
    payload: PaymentCreateRequest,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    idempotency_key: Annotated[
        str, Header(alias="Idempotency-Key", min_length=1, max_length=128, pattern=r"^\S+$")
    ],
) -> PaymentCreateResponse:
    service = PaymentService(session)
    payment = await service.create_payment(payload, idempotency_key)

    return PaymentCreateResponse(
        payment_id=payment.id,
        status=payment.status.value,
        created_at=payment.created_at,
    )


@router.get("/{payment_id}", response_model=PaymentResponse)
async def get_payment(
    payment_id: UUID,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> PaymentResponse:
    service = PaymentService(session)
    payment = await service.get_payment(payment_id)

    return PaymentResponse(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.metadata_json,
        status=payment.status.value,
        idempotency_key=payment.idempotency_key,
        webhook_url=payment.webhook_url,
        processed_at=payment.processed_at,
        created_at=payment.created_at,
        updated_at=payment.updated_at,
    )
