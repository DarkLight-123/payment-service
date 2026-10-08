"""create payments and outbox tables

Revision ID: 0001_create_payments_and_outbox
Revises: None
Create Date: 2026-03-25 00:00:00
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0001_create_payments_and_outbox"
down_revision = None
branch_labels = None
depends_on = None


payment_status_enum = postgresql.ENUM(
    "pending",
    "succeeded",
    "failed",
    name="payment_status",
    create_type=False,
)

outbox_status_enum = postgresql.ENUM(
    "pending",
    "published",
    "failed",
    name="outbox_status",
    create_type=False,
)


def upgrade() -> None:
    payment_status_enum.create(op.get_bind(), checkfirst=True)
    outbox_status_enum.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "payments",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("description", sa.String(length=255), nullable=False),
        sa.Column(
            "metadata_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("status", payment_status_enum, nullable=False, server_default="pending"),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("webhook_url", sa.String(length=1024), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("TIMEZONE('utc', now())"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("TIMEZONE('utc', now())"),
        ),
        sa.UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
    )
    op.create_index("ix_payments_status", "payments", ["status"])
    op.create_index("ix_payments_created_at", "payments", ["created_at"])

    op.create_table(
        "outbox_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("aggregate_type", sa.String(length=100), nullable=False),
        sa.Column("aggregate_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", outbox_status_enum, nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("TIMEZONE('utc', now())"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("TIMEZONE('utc', now())"),
        ),
    )
    op.create_index("ix_outbox_events_status", "outbox_events", ["status"])
    op.create_index("ix_outbox_events_created_at", "outbox_events", ["created_at"])


def downgrade() -> None:
    op.drop_index("ix_outbox_events_created_at", table_name="outbox_events")
    op.drop_index("ix_outbox_events_status", table_name="outbox_events")
    op.drop_table("outbox_events")

    op.drop_index("ix_payments_created_at", table_name="payments")
    op.drop_index("ix_payments_status", table_name="payments")
    op.drop_table("payments")

    outbox_status_enum.drop(op.get_bind(), checkfirst=True)
    payment_status_enum.drop(op.get_bind(), checkfirst=True)
