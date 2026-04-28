# ADR-008: Механизмы обеспечения надежной доставки событий

## Контекст

Событийная архитектура "GoFuture" обрабатывает ~150K событий/сек на пике (из которых ~100K - обновления GPS-позиций водителей). Потеря критических событий (BookingCreated, PaymentCompleted) приводит к потере денег и нарушению бизнес-процессов. Потеря некритических событий (NotificationSent, AnalyticsEvent) допустима в единичных случаях, но не массово.

Требование из NFR (ADR-001): потеря событий <= 0,001% (at-least-once delivery).

## Требования

Обеспечить надежную доставку событий между сервисами с гарантией at-least-once и идемпотентной обработкой.

## Решение

### 1. Transactional Outbox Pattern

Проблема: Сервис должен атомарно обновить свою БД И опубликовать событие в Kafka. Без специального механизма возможны два сценария потери:

- БД обновлена, но событие не отправлено (crash между записью и publish).
- Событие отправлено, но БД не обновлена (маловероятно с Kafka acks=all, но возможно).

Решение: Outbox-таблица в БД сервиса

```sql
CREATE TABLE outbox_events (
    event_id        UUID PRIMARY KEY,
    event_type      VARCHAR(100) NOT NULL,
    topic           VARCHAR(200) NOT NULL,
    partition_key   VARCHAR(200) NOT NULL,
    payload         JSONB NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMP,             -- NULL пока не отправлено
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED'))
);

CREATE INDEX idx_outbox_pending ON outbox_events (status, created_at)
    WHERE status = 'PENDING';
```

Механизм работы:

Диаграмма: [diagrams/outbox-pattern.puml](diagrams/outbox-pattern.puml)

1. Бизнес-операция + запись события - в одной БД-транзакции (`BEGIN; INSERT INTO bookings ...; INSERT INTO outbox_events ...; COMMIT;`).
2. Outbox Relay (Debezium CDC) читает WAL PostgreSQL и отправляет изменения `outbox_events` в Kafka topic.
3. После подтверждения публикации: `UPDATE outbox_events SET status = 'PUBLISHED'`.

Два варианта relay:

| Вариант                      | Описание                                                                                                      | Плюсы                                                        | Минусы                                     |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------ |
| Debezium CDC (основной)      | Debezium читает WAL PostgreSQL и отправляет изменения outbox_events в Kafka                                   | Без polling, minimal lag (~100ms), не нагружает БД запросами | Зависимость от Debezium, сложнее настройка |
| Polling Publisher (fallback) | Background job каждые 500ms: `SELECT FROM outbox_events WHERE status = 'PENDING'` -> publish -> update status | Простая реализация, нет зависимостей                         | Нагрузка на БД, lag до 500ms               |

Выбор: Debezium CDC как основной механизм. Polling Publisher как fallback при проблемах с Debezium.

### 2. Idempotent Consumer

Проблема: At-least-once delivery означает, что событие может быть доставлено повторно (rebalance consumer group, retry, network issue). Повторная обработка PaymentCompleted -> двойное списание.

Решение: Таблица обработанных событий + дедупликация

```sql
CREATE TABLE processed_events (
    event_id        UUID PRIMARY KEY,
    event_type      VARCHAR(100) NOT NULL,
    processed_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    consumer_group  VARCHAR(100) NOT NULL
);

-- TTL: автоочистка через 7 дней
CREATE INDEX idx_processed_events_ttl ON processed_events (processed_at);
```

Механизм работы:

```python
def handle_event(event):
    # 1. Проверка дедупликации
    if is_already_processed(event.event_id, consumer_group):
        log.info(f"Duplicate event {event.event_id}, skipping")
        return ACK

    # 2. Обработка события + отметка - в одной транзакции
    with db.transaction():
        process_business_logic(event)
        mark_as_processed(event.event_id, consumer_group)

    # 3. Commit Kafka offset
    return ACK
```

Для высоконагруженных потоков (driver.location.updates):

- Вместо таблицы - Bloom filter в Redis (probabilistic, ~0.01% false positive допустимо для GPS-обновлений).
- Или Last-Writer-Wins: обновляем позицию по timestamp, повторное событие с тем же или более старым timestamp игнорируется.

### 3. Dead Letter Queue (DLQ)

Проблема: Некоторые события не могут быть обработаны (ошибка десериализации, бизнес-валидация, недоступность зависимости после всех retry). Такие события не должны блокировать обработку остальных.

Механизм:

Диаграмма: [diagrams/dlq-retry-flow.puml](diagrams/dlq-retry-flow.puml)

Конфигурация retry:

| Попытка | Задержка | Описание                                  |
| ------- | -------- | ----------------------------------------- |
| 1       | 1 сек    | Транзиентная ошибка (таймаут сети)        |
| 2       | 10 сек   | Кратковременная недоступность зависимости |
| 3       | 60 сек   | Продолжительная недоступность             |
| DLQ     | -        | Ручное расследование                      |

Именование DLQ-топиков: `{original_topic}.dlq`  
Пример: `booking.trip.lifecycle.v1.dlq`

DLQ-событие содержит:

```json
{
  "original_event": {
    /* оригинальное событие */
  },
  "error": {
    "message": "Fraud service unavailable after 3 retries",
    "exception": "ConnectionTimeoutError",
    "stack_trace": "...",
    "retry_count": 3,
    "first_attempt_at": "2026-04-13T14:30:00Z",
    "last_attempt_at": "2026-04-13T14:31:11Z"
  },
  "consumer_group": "booking-saga-orchestrator",
  "original_topic": "saga.booking.replies.v1",
  "original_partition": 12,
  "original_offset": 98765
}
```

Алертинг: Любое событие в DLQ -> алерт в Slack/PagerDuty (severity зависит от топика: payment._.dlq -> P1, notification._.dlq -> P3).

### 4. Конфигурация Kafka для надежности

#### Producer

```properties
# Гарантия записи на все реплики
acks=all

# Идемпотентный producer (дедупликация на стороне брокера)
enable.idempotence=true

# Retry при transient-ошибках
retries=2147483647
delivery.timeout.ms=120000

# Порядок сообщений (с идемпотентностью max.in.flight=5 безопасен)
max.in.flight.requests.per.connection=5

# Сжатие (снижает сетевой трафик на ~70%)
compression.type=lz4

# Батчинг для throughput
batch.size=65536
linger.ms=5
```

#### Broker

```properties
# Минимум реплик для подтверждения записи
min.insync.replicas=2

# Фактор репликации (3 брокера в кластере)
default.replication.factor=3

# Не допускать unclean leader election (потеря данных)
unclean.leader.election.enable=false

# Log flush (не отключаем fsync для финансовых топиков)
# Для payment.* топиков: flush.messages=1
```

#### Consumer

```properties
# Ручной commit offset (после успешной обработки)
enable.auto.commit=false

# Изоляция транзакционных сообщений
isolation.level=read_committed

# Максимальный batch для обработки
max.poll.records=500
max.poll.interval.ms=300000

# Heartbeat и session timeout
session.timeout.ms=30000
heartbeat.interval.ms=10000
```

### 5. Гарантии доставки по типу событий

| Категория                | Примеры событий                                    | Гарантия                        | Механизм                                                                         |
| ------------------------ | -------------------------------------------------- | ------------------------------- | -------------------------------------------------------------------------------- |
| Критические (финансовые) | PaymentCompleted, PayoutCompleted, RefundProcessed | Exactly-once (семантически)     | Outbox + idempotent consumer + acks=all + min.isr=2                              |
| Критические (бизнес)     | BookingCreated, DriverAssigned, TripCompleted      | At-least-once + идемпотентность | Outbox + idempotent consumer                                                     |
| Важные                   | PriceCalculated, SurgeUpdated, FraudCheckPassed    | At-least-once                   | Outbox + DLQ                                                                     |
| Информационные           | NotificationSent, DriverLocationUpdated            | At-least-once (best effort)     | Direct produce + DLQ (для Notification), fire-and-forget допустим (для Location) |

### 6. Ordering Guarantees

Порядок в рамках одной сущности гарантируется:

- Ключ партиционирования = entity_id (booking_id, driver_id и т.д.).
- Kafka гарантирует порядок внутри одной партиции.
- `max.in.flight.requests.per.connection=5` + `enable.idempotence=true` -> порядок сохраняется при retry.

Порядок между разными сущностями НЕ гарантируется (и не нужен):

- BookingCreated для разных поездок могут обрабатываться в произвольном порядке.

### 7. Мониторинг надежности доставки

| Метрика                             | Алерт          | Описание                                          |
| ----------------------------------- | -------------- | ------------------------------------------------- |
| `outbox_pending_count`              | > 1000         | Outbox relay отстает                              |
| `outbox_pending_age_seconds`        | > 30           | Старейшее неотправленное событие                  |
| `dlq_messages_total`                | > 0 (за 5 мин) | Событие попало в DLQ                              |
| `consumer_lag`                      | > 10000        | Consumer не успевает обрабатывать                 |
| `duplicate_events_total`            | > 100/мин      | Высокий уровень дупликатов (проблема на producer) |
| `kafka_under_replicated_partitions` | > 0            | Потенциальная потеря данных при отказе брокера    |

## Альтернативы

1. Прямая публикация в Kafka (без Outbox) - проще, но не гарантирует атомарность "запись в БД + публикация события". При crash между двумя операциями данные рассогласуются. Допустимо только для некритических событий (DriverLocationUpdated).
2. Kafka Transactions (Exactly-Once Semantics) - гарантирует exactly-once между produce и consume, но работает только для Kafka->Kafka потоков (Kafka Streams). Не подходит для Kafka->DB flow.
3. Saga с синхронными вызовами (без Kafka) - ниже latency (~50 мс вместо ~150 мс), но при отказе сервиса запрос пропадает. Kafka обеспечивает durable queue с retry.

## Компромиссы

- Outbox + Debezium добавляют ~50-100 мс latency (WAL -> Debezium -> Kafka) по сравнению с прямой публикацией. Для финансовых событий приемлемо, для DriverLocationUpdated - используем прямую публикацию.
- Idempotent consumer с таблицей нагружает БД (write на каждое событие). Для высоконагруженных потоков используем Bloom filter или Last-Writer-Wins.
- DLQ требует процесса ручной обработки (on-call дежурный). Митигация: автоматизация reprocessing для типовых ошибок.
- acks=all + min.isr=2 снижает throughput producer на ~30% по сравнению с acks=1. Для финансовых топиков - обязательно; для location updates - допустимо acks=1.
