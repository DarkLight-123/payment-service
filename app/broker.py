from faststream.rabbit import Channel, ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.core.config import settings

payments_exchange = RabbitExchange(
    name=settings.payments_exchange,
    type=ExchangeType.DIRECT,
    durable=True,
)

payments_dlq = RabbitQueue(
    name=settings.payments_dlq,
    durable=True,
)

payments_queue = RabbitQueue(
    name=settings.payments_queue,
    durable=True,
    routing_key=settings.payments_routing_key,
    arguments={
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": settings.payments_dlq,
    },
)

broker = RabbitBroker(
    settings.rabbitmq_url,
    default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
)


async def declare_payment_topology() -> None:
    await broker.declare_queue(payments_dlq)
    exchange = await broker.declare_exchange(payments_exchange)
    queue = await broker.declare_queue(payments_queue)
    await queue.bind(exchange, routing_key=settings.payments_routing_key)
