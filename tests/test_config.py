import pytest
from pydantic import ValidationError
from sqlalchemy import make_url
from yarl import URL

from app.core.config import Settings


def test_connection_urls_escape_credentials():
    config = Settings(
        _env_file=None,
        postgres_password="p@ss:/%word",
        rabbitmq_password="p@ss:/%word",
        rabbitmq_vhost="/test/vhost",
        database_url=None,
        sync_database_url=None,
    )
    assert make_url(config.effective_database_url).password == "p@ss:/%word"
    assert make_url(config.effective_sync_database_url).password == "p@ss:/%word"
    assert URL(config.rabbitmq_url).password == "p@ss:/%word"
    assert URL(config.rabbitmq_url).path == "/" + config.rabbitmq_vhost


@pytest.mark.parametrize(
    "changes",
    [
        {"outbox_poll_interval_seconds": 0},
        {"max_processing_attempts": 0},
        {"payment_success_rate": 1.1},
        {"payment_processing_min_delay_seconds": 6, "payment_processing_max_delay_seconds": 5},
    ],
)
def test_invalid_worker_settings(changes):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **changes)
