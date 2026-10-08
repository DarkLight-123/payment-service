from urllib.parse import quote

from pydantic import Field, computed_field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import URL


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "payment-processing-service"
    app_env: str = "dev"
    api_v1_prefix: str = "/api/v1"
    api_key: str = Field(default="dev-api-key", min_length=1)

    postgres_db: str = "payments_db"
    postgres_user: str = "payments_user"
    postgres_password: str = "payments_pass"
    postgres_host: str = "localhost"
    postgres_port: int = 5432

    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    rabbitmq_user: str = "payments_user"
    rabbitmq_password: str = "payments_pass"
    rabbitmq_vhost: str = "/"

    database_url: str | None = None
    sync_database_url: str | None = None

    outbox_poll_interval_seconds: float = Field(default=1.0, gt=0)
    outbox_batch_size: int = Field(default=100, gt=0)
    payments_exchange: str = "payments"
    payments_routing_key: str = "payments.new"
    payments_queue: str = "payments.new"
    payments_dlq: str = "payments.dlq"

    webhook_timeout_seconds: float = Field(default=10.0, gt=0)
    payment_processing_min_delay_seconds: int = Field(default=2, ge=0)
    payment_processing_max_delay_seconds: int = Field(default=5, ge=0)
    payment_success_rate: float = Field(default=0.9, ge=0, le=1)
    max_processing_attempts: int = Field(default=3, gt=0)

    @model_validator(mode="after")
    def validate_delays(self) -> "Settings":
        if self.payment_processing_min_delay_seconds > self.payment_processing_max_delay_seconds:
            raise ValueError("Minimum processing delay must not exceed maximum delay")
        return self

    @computed_field
    @property
    def rabbitmq_url(self) -> str:
        return (
            f"amqp://{quote(self.rabbitmq_user, safe='')}:{quote(self.rabbitmq_password, safe='')}@"
            f"{self.rabbitmq_host}:{self.rabbitmq_port}/{quote(self.rabbitmq_vhost, safe='')}"
        )

    @computed_field
    @property
    def effective_database_url(self) -> str:
        if self.database_url:
            return self.database_url
        return self._database_url("postgresql+asyncpg")

    @computed_field
    @property
    def effective_sync_database_url(self) -> str:
        if self.sync_database_url:
            return self.sync_database_url
        return self._database_url("postgresql+psycopg")

    def _database_url(self, driver: str) -> str:
        return URL.create(
            driver,
            username=self.postgres_user,
            password=self.postgres_password,
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_db,
        ).render_as_string(hide_password=False)


settings = Settings()
