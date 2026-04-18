# ADR-006: Модель доменных событий и топики Kafka

## Контекст

Целевая архитектура "GoFuture" (см. ADR-002) включает 8 доменных сервисов, взаимодействующих как синхронно (gRPC на критическом пути), так и асинхронно (через событийную шину). Для обработки 500K конкурентных поездок и динамического ценообразования в реальном времени необходима event-driven архитектура на базе Apache Kafka.

Ключевые требования:

- Гарантия порядка событий в рамках одной поездки/водителя.
- Партиционирование по регионам для мультирегионального развертывания.
- Пропускная способность: ~50K событий/сек при пиковой нагрузке.
- Обработка событий в реальном времени (latency < 1 с для критических потоков).

## Требования

Определить каталог доменных событий, схему топиков Kafka с партиционированием и потребителей для обработки событий в реальном времени.

## Решение

### 1. Каталог доменных событий

#### Booking Domain

| Событие          | Описание                                       | Ключ партиционирования | Потребители                                     |
| ---------------- | ---------------------------------------------- | ---------------------- | ----------------------------------------------- |
| BookingCreated   | Создан новый заказ (пассажир нажал "Заказать") | booking_id             | Driver, Pricing, Fraud, Analytics, Notification |
| BookingAccepted  | Водитель принял заказ                          | booking_id             | Notification, Analytics                         |
| BookingCancelled | Заказ отменен (пассажиром или системой)        | booking_id             | Driver, Payments, Notification, Analytics       |
| TripStarted      | Поездка началась (водитель начал маршрут)      | booking_id             | Pricing, Analytics, Notification                |
| TripCompleted    | Поездка завершена                              | booking_id             | Payments, Analytics, Notification, Pricing      |
| TripRouteUpdated | Обновление маршрута во время поездки           | booking_id             | Analytics                                       |

#### Driver Domain

| Событие               | Описание                                  | Ключ партиционирования | Потребители                                   |
| --------------------- | ----------------------------------------- | ---------------------- | --------------------------------------------- |
| DriverOnline          | Водитель вышел на линию                   | driver_id              | Geography, Analytics                          |
| DriverOffline         | Водитель ушел с линии                     | driver_id              | Geography, Analytics                          |
| DriverLocationUpdated | Обновление GPS-координат (каждые 3-5 сек) | driver_id              | Geography, Pricing (агрегированно), Analytics |
| DriverAssigned        | Водитель назначен на заказ                | driver_id              | Booking, Notification, Analytics              |
| DriverRatingUpdated   | Обновлен рейтинг водителя                 | driver_id              | Analytics                                     |

#### Pricing Domain

| Событие               | Описание                            | Ключ партиционирования | Потребители               |
| --------------------- | ----------------------------------- | ---------------------- | ------------------------- |
| PriceCalculated       | Рассчитана стоимость поездки        | booking_id             | Booking, Analytics        |
| SurgeUpdated          | Обновлен surge-коэффициент для зоны | zone_id                | Analytics, Geography      |
| DemandForecastUpdated | ML-модель обновила прогноз спроса   | region_id              | Pricing (self), Analytics |

#### Payments Domain

| Событие          | Описание                      | Ключ партиционирования | Потребители                      |
| ---------------- | ----------------------------- | ---------------------- | -------------------------------- |
| PaymentInitiated | Инициирован платеж            | payment_id             | Fraud, Analytics                 |
| PaymentCompleted | Платеж успешно обработан      | payment_id             | Booking, Notification, Analytics |
| PaymentFailed    | Ошибка платежа                | payment_id             | Booking, Notification, Analytics |
| RefundProcessed  | Обработан возврат             | payment_id             | Notification, Analytics          |
| PayoutInitiated  | Инициирована выплата водителю | payout_id              | Analytics                        |
| PayoutCompleted  | Выплата успешно проведена     | payout_id              | Notification, Analytics          |
| PayoutFailed     | Ошибка выплаты                | payout_id              | Notification, Analytics          |

#### Notification Domain

| Событие            | Описание                    | Ключ партиционирования | Потребители |
| ------------------ | --------------------------- | ---------------------- | ----------- |
| NotificationSent   | Уведомление отправлено      | notification_id        | Analytics   |
| NotificationFailed | Ошибка отправки уведомления | notification_id        | Analytics   |

#### Geography Domain

| Событие               | Описание                               | Ключ партиционирования | Потребители        |
| --------------------- | -------------------------------------- | ---------------------- | ------------------ |
| GeozoneEntered        | Водитель въехал в зону                 | driver_id              | Pricing, Analytics |
| GeozoneExited         | Водитель покинул зону                  | driver_id              | Pricing, Analytics |
| DriverLocationIndexed | Позиция водителя проиндексирована в ES | driver_id              | (внутреннее)       |

#### Fraud Domain

| Событие          | Описание                             | Ключ партиционирования | Потребители                                      |
| ---------------- | ------------------------------------ | ---------------------- | ------------------------------------------------ |
| FraudCheckPassed | Проверка на фрод пройдена            | entity_id              | Booking/Payments (saga), Analytics               |
| FraudDetected    | Обнаружена подозрительная активность | entity_id              | Booking/Payments (saga), Notification, Analytics |
| UserBlocked      | Пользователь заблокирован            | user_id                | Identity, Notification, Analytics                |

#### Analytics Domain

| Событие               | Описание                       | Ключ партиционирования | Потребители    |
| --------------------- | ------------------------------ | ---------------------- | -------------- |
| DemandForecastUpdated | Обновлен прогноз спроса в зоне | zone_id                | Pricing        |
| AnomalyDetected       | ML обнаружила аномалию         | region_id              | SRE (alerting) |

### 2. Формат событий (Event Envelope)

Все события используют единый формат обертки, сериализованный в Avro (через Schema Registry):

```json
{
  "event_id": "uuid-v4",
  "event_type": "BookingCreated",
  "event_version": 1,
  "timestamp": "2026-04-12T14:30:00.123Z",
  "source_service": "booking-service",
  "region_id": "sea-sg",
  "correlation_id": "uuid-v4 (trace-id)",
  "causation_id": "uuid-v4 (id вызвавшего события)",
  "metadata": {
    "user_id": "uuid",
    "tenant_id": "partner-123",
    "idempotency_key": "uuid-v4"
  },
  "payload": {
    // Доменно-специфичные данные
  }
}
```

Ключевые поля:

- event_id - уникальный идентификатор для дедупликации (idempotent consumer).
- correlation_id - сквозной trace-id для distributed tracing.
- causation_id - id события-причины для построения цепочки каузальности.
- region_id - регион, используется для маршрутизации и партиционирования.
- idempotency_key - ключ для idempotent обработки.

### 3. Схема топиков Kafka

#### Конвенция именования

```
{domain}.{entity}.{event-category}.{version}
```

Примеры: `booking.trip.lifecycle.v1`, `driver.location.updates.v1`

#### Карта топиков

| Топик                              | Партиции | Ключ                 | Retention | Throughput (пик) | Описание                                                        |
| ---------------------------------- | -------- | -------------------- | --------- | ---------------- | --------------------------------------------------------------- |
| `booking.trip.lifecycle.v1`        | 64       | booking_id           | 7 дней    | ~5K msg/s        | BookingCreated, Accepted, Cancelled, TripStarted, TripCompleted |
| `booking.trip.route.v1`            | 32       | booking_id           | 3 дня     | ~10K msg/s       | TripRouteUpdated (GPS-трек поездки)                             |
| `driver.status.lifecycle.v1`       | 32       | driver_id            | 7 дней    | ~1K msg/s        | DriverOnline, DriverOffline, DriverAssigned, RatingUpdated      |
| `driver.location.updates.v1`       | 128      | driver_id            | 1 час     | ~100K msg/s      | DriverLocationUpdated (каждые 3-5 сек, ~500K водителей)         |
| `pricing.price.events.v1`          | 32       | booking_id / zone_id | 7 дней    | ~5K msg/s        | PriceCalculated, SurgeUpdated                                   |
| `pricing.demand.forecast.v1`       | 16       | zone_id              | 1 день    | ~100 msg/s       | DemandForecastUpdated                                           |
| `payment.transaction.lifecycle.v1` | 32       | payment_id           | 30 дней   | ~3K msg/s        | PaymentInitiated, Completed, Failed, RefundProcessed            |
| `payment.payout.lifecycle.v1`      | 16       | payout_id            | 30 дней   | ~500 msg/s       | PayoutInitiated, Completed, Failed                              |
| `notification.delivery.events.v1`  | 16       | notification_id      | 3 дня     | ~10K msg/s       | NotificationSent, NotificationFailed                            |
| `geography.zone.events.v1`         | 16       | driver_id            | 1 день    | ~5K msg/s        | GeozoneEntered, GeozoneExited                                   |
| `fraud.check.events.v1`            | 16       | entity_id            | 7 дней    | ~2K msg/s        | FraudCheckPassed, FraudDetected, UserBlocked                    |
| `analytics.ml.events.v1`           | 8        | region_id            | 1 день    | ~100 msg/s       | AnomalyDetected                                                 |
| `saga.booking.commands.v1`         | 64       | booking_id           | 3 дня     | ~5K msg/s        | Команды Booking Saga Orchestrator                               |
| `saga.booking.replies.v1`          | 64       | booking_id           | 3 дня     | ~5K msg/s        | Ответы участников Booking Saga                                  |
| `saga.payment.commands.v1`         | 32       | payment_id           | 3 дня     | ~3K msg/s        | Команды Payment Saga Orchestrator                               |
| `saga.payment.replies.v1`          | 32       | payment_id           | 3 дня     | ~3K msg/s        | Ответы участников Payment Saga                                  |
| `*.dlq`                            | 4        | original_key         | 30 дней   | <1 msg/s         | Dead Letter Queue для каждого топика                            |

#### Стратегия партиционирования

Двухуровневое партиционирование:

```
Уровень 1: Кластер Kafka per region: kafka-sea (Юго-Восточная Азия), kafka-sa (Южная Америка), kafka-ru (Россия/СНГ)

Уровень 2: Партиции внутри кластера по entity_id: partition = hash(booking_id) % num_partitions
```

- Региональная изоляция - каждый регион имеет свой Kafka-кластер. Это обеспечивает суверенитет данных и минимизирует cross-region латентность.
- Партиционирование по entity_id - события одной поездки/водителя всегда попадают в одну партицию -> гарантия порядка.
- Кросс-региональная репликация - только для агрегированных аналитических событий (Kafka MirrorMaker 2 -> центральный analytics-кластер).

Расчет количества партиций для `driver.location.updates.v1`:

- 500K активных водителей \* обновление каждые 5 сек = 100K msg/s.
- 1 партиция обрабатывает ~1K msg/s (consumer throughput).
- 128 партиций -> до 128 consumer-инстансов -> ~128K msg/s (запас 28%).

### 4. Потребители для обработки событий в реальном времени

Диаграмма всех stream processors: [diagrams/stream-processors.puml](diagrams/stream-processors.puml)

#### 4.1 Geography Stream Processor (Flink)

- Задача: Обработка 100K GPS-обновлений/сек, индексация в Elasticsearch, детектирование входа/выхода из геозон.
- Технология: Apache Flink (stateful stream processing, low latency).
- Windowing: Tumbling window 5 сек для батчевого обновления ES-индекса.
- State: Keyed state по driver_id (текущая геозона для детекции входа/выхода).

#### 4.2 Surge Pricing Processor (Flink)

- Задача: Расчет surge-коэффициента на основе соотношения спроса (BookingCreated) и предложения (DriverLocationUpdated) в каждой геозоне.
- Windowing: Sliding window 5 мин, шаг 30 сек.
- Алгоритм: `surge = f(demand_count / supply_count)` с учетом исторических данных и ML-прогноза.
- Latency: < 30 сек от изменения спроса/предложения до обновления surge.

#### 4.3 Demand Forecast Processor (Flink)

- Задача: ML-инференс для прогнозирования спроса в зоне на ближайшие 15-30 мин.
- Windowing: Tumbling window 5 мин.
- Модель: Предобученная ML-модель (загружается из Model Registry), feature engineering в Flink.

#### 4.4 Smart Driver Matching Processor (Flink)

- Задача: "Умное" перераспределение водителей - вместо ближайшего выбирается оптимальный водитель с учетом прогноза спроса, предотвращение "горячих зон".
- Алгоритм: Scoring по: расстояние + прогноз спроса в зоне водителя + текущий surge + рейтинг.
- Latency: < 3 сек от BookingCreated до DriverAssigned.

#### 4.5 Fraud Detection Processor (Flink)

- Задача: Real-time fraud scoring на основе поведенческих паттернов.
- Windowing: Session window per user (5 мин неактивности -> закрытие сессии).
- Features: Частота заказов, отмены, аномальные маршруты, скорость транзакций.

#### 4.6 Analytics Aggregator (Flink -> ClickHouse)

- Задача: Агрегация всех бизнес-событий для BI и отчетов.
- Windowing: Tumbling window 1 мин (минутные агрегаты) + 1 час (часовые).
- Sink: ClickHouse через JDBC sink (batch insert каждые 10 сек).

#### 4.7 Notification Dispatcher (Kafka Consumer Group)

- Задача: Маппинг доменных событий на шаблоны уведомлений и отправка через push-провайдеров.
- Технология: Kafka Consumer (не нужен stateful processing, простой mapping).
- Retry: 3 попытки с exponential backoff, затем DLQ.

### 5. Сводная схема потоков данных

Диаграмма: [diagrams/event-data-flow.puml](diagrams/event-data-flow.puml)

## Альтернативы

1. RabbitMQ вместо Kafka - проще в эксплуатации, но не поддерживает replay событий, log compaction, и плохо масштабируется для 100K+ msg/s. Не подходит для event streaming.
2. Kafka Streams вместо Flink - проще деплой (library, не cluster), но ограничен JVM, менее гибкий windowing, хуже для complex event processing. Flink выбран для тяжелых стримовых задач (GeoIndexer, Surge Pricing), Kafka Consumer - для простых (Notification).
3. Единый топик per domain (вместо per entity) - проще управлять, но теряем granularity: consumer вынужден фильтровать ненужные события, увеличивается нагрузка.
4. Глобальный Kafka-кластер (вместо per-region) - проще управлять, но cross-region латентность 100-300 мс неприемлема для real-time событий; нарушает суверенитет данных.

## Компромиссы

- 128 партиций для driver.location - много партиций усложняют ребалансировку и увеличивают метаданные на брокерах, но необходимы для параллелизма при 100K msg/s.
- Retention 1 час для driver.location - теряем историю позиций, но экономим значительный объем хранилища (~100K msg/s _ 3600s _ ~200B ≈ 72GB/ч). Историю сохраняет Analytics.
- Flink для стримовых процессоров - дополнительный кластер для эксплуатации, но обеспечивает exactly-once семантику, stateful processing и sub-second latency.
- Avro + Schema Registry - дополнительная зависимость, но строгая типизация предотвращает ошибки несовместимости схем и экономит трафик (бинарный формат компактнее JSON в 3-5 раз).
- Separate Kafka per region - 3\* операционные затраты на кластеры, но обеспечивает data sovereignty и минимальную латентность.
