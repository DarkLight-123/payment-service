# Payment service

Асинхронная обработка платежей на FastAPI, PostgreSQL и RabbitMQ (FastStream).
Запрос создаёт платёж и событие в одной транзакции. Outbox publisher отправляет
событие в RabbitMQ, consumer эмулирует платёжный шлюз и уведомляет клиента через webhook.

## Запуск

Нужны Docker и Docker Compose.

```bash
cp .env.example .env
docker compose up --build --wait
```

В PowerShell вместо `cp` можно использовать `Copy-Item .env.example .env`.
Миграции применяются при запуске API. Consumer запускается после готовности API.

- API: http://localhost:8000
- RabbitMQ Management: http://localhost:15672
- Ключ API из примера: `dev-api-key`
- RabbitMQ: `payments_user` / `payments_pass`

Все маршруты, включая `/health`, `/docs` и `/openapi.json`, требуют заголовок
`X-API-Key`. При работе со Swagger этот заголовок нужно передавать и для загрузки
документации, например через настройку заголовков в браузере.

Остановка: `docker compose down`. Данные сохраняются в именованных томах.
Команда `docker compose down -v` удаляет данные PostgreSQL и RabbitMQ.

## API

Создание платежа:

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: dev-api-key' \
  -H 'Idempotency-Key: order-001' \
  -d '{"amount":"1500.50","currency":"RUB","description":"Order 001","metadata":{"order_id":"001"},"webhook_url":"https://example.com/webhook"}'
```

Ответ: `202 Accepted`, `payment_id`, `status`, `created_at`.
Для проверки webhook укажите доступный consumer URL вместо `example.com`.
В Windows используйте `curl.exe`; переносы строк в примере рассчитаны на Bash.

Получение платежа:

```bash
curl 'http://localhost:8000/api/v1/payments/<payment_id>' \
  -H 'X-API-Key: dev-api-key'
```

Статус: `pending`, `succeeded` или `failed`. Ответ также содержит сумму,
валюту, описание, метаданные, ключ идемпотентности, webhook URL и даты.
Сумма передаётся строкой, чтобы сохранить точность decimal.

Повторный POST с тем же ключом и тем же телом возвращает существующий платёж.
Если тело отличается, сервис отвечает `409 Conflict`. Ключ обязателен,
его длина — от 1 до 128 символов, без пробелов. Поддерживаются RUB, USD и EUR;
регистр валюты нормализуется. Неверные данные дают `422`, отсутствующий или
неверный API-ключ — `401`, неизвестный платёж — `404`.

## Обработка и доставка

В БД две основные таблицы: `payments` и `outbox`.
Publisher выбирает события с блокировкой `FOR UPDATE SKIP LOCKED`, публикует
persistent-сообщения с publisher confirms и отмечает их отправленными после
подтверждения брокера. Ошибка публикации оставляет событие в Outbox для следующей попытки.
Обменник `payments`, очередь `payments.new` и DLQ `payments.dlq` создаются при старте.

Один consumer обрабатывает сообщения последовательно. Шлюз эмулируется задержкой
2–5 секунд: 90% платежей получают `succeeded`, 10% — `failed`. Оба результата
сохраняются в БД и отправляются клиенту. Бизнес-ошибка платежа не означает ошибку доставки.

Webhook отправляется POST-запросом с `payment_id`, `status`, `amount`, `currency`,
`description`, `metadata` и `processed_at`. При ошибке предусмотрены три попытки
всего, с задержками 1 и 2 секунды перед повторными. Результат оплаты при этом
не пересчитывается. После третьей ошибки сообщение отклоняется и попадает в DLQ.

Успешная отправка отмечается в `webhook_sent_at`. Повторно доставленное событие
не запускает уже завершённую оплату и не отправляет отмеченный webhook.
Доставка имеет семантику at-least-once: сбой между отправкой сообщения и фиксацией
в БД может привести к повтору. Получателю webhook нужно учитывать `payment_id`
для защиты от дублей. Счётчик попыток consumer относится к одной доставке сообщения;
после аварийного перезапуска незавершённая доставка может быть повторена брокером.

## Проверка

Проверка запущенного Compose, включая две неудачные отправки webhook и успешную третью:

```bash
docker compose exec -T api python scripts/smoke_test.py --webhook-host api
```

Для локальных тестов нужен Python 3.12:

```bash
python -m venv .venv
# Активируйте .venv для своей оболочки.
python -m pip install -r requirements.lock -e '.[dev]'
pytest -q
ruff check .
ruff format --check .
```

По умолчанию выполняются тесты API, валидации и consumer. Интеграционные тесты
используют настоящие PostgreSQL и RabbitMQ. Для них задайте параметры подключения,
`RUN_INTEGRATION_TESTS=1` и отдельную БД с именем, заканчивающимся на `_test`.
Эти тесты очищают таблицы тестовой БД.

GitHub Actions выполняет полный набор тестов, проверяет миграции и отдельно
собирает Docker-образ, запускает Compose и сценарий с webhook.

## Конфигурация

Параметры находятся в `.env.example`. Подключения можно задать через
`POSTGRES_*` и `RABBITMQ_*` либо переопределить `DATABASE_URL` и `SYNC_DATABASE_URL`.
Таймаут webhook, интервал опроса Outbox и параметры эмуляции также настраиваются
через окружение. Зависимости зафиксированы в `requirements.lock`.
