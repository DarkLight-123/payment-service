"""Track webhook delivery and align the outbox table name."""

import sqlalchemy as sa

from alembic import op

revision = "0002_webhook_delivery"
down_revision = "0001_create_payments_and_outbox"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.rename_table("outbox_events", "outbox")
    op.add_column("payments", sa.Column("webhook_sent_at", sa.DateTime(timezone=True)))
    for table in ("payments", "outbox"):
        for column in ("created_at", "updated_at"):
            op.alter_column(table, column, server_default=sa.text("now()"))


def downgrade() -> None:
    for table in ("payments", "outbox"):
        for column in ("created_at", "updated_at"):
            op.alter_column(table, column, server_default=sa.text("TIMEZONE('utc', now())"))
    op.drop_column("payments", "webhook_sent_at")
    op.rename_table("outbox", "outbox_events")
