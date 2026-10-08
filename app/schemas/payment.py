from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

Currency = Literal["RUB", "USD", "EUR"]
Status = Literal["pending", "succeeded", "failed"]


class PaymentCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Decimal = Field(..., gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: str = Field(..., min_length=1, max_length=255)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl = Field(..., max_length=1024)

    @field_validator("currency", mode="before")
    @classmethod
    def normalize_currency(cls, value: Any) -> Any:
        return value.upper() if isinstance(value, str) else value


class PaymentCreateResponse(BaseModel):
    payment_id: UUID
    status: Status
    created_at: datetime


class PaymentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: UUID
    amount: Decimal
    currency: str
    description: str
    metadata: dict[str, Any]
    status: Status
    idempotency_key: str
    webhook_url: str
    processed_at: datetime | None
    created_at: datetime
    updated_at: datetime
