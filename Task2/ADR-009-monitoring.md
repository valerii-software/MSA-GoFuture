# ADR-009: Мониторинг событийной платформы

## Контекст

Событийная платформа "GoFuture" обрабатывает ~150K событий/сек на пике через Kafka, Apache Flink и множество consumer-групп. Платформа включает Saga-оркестраторы, stream processors и интеграции с внешними системами. Для обеспечения SLA 99,95% необходим комплексный мониторинг, который позволяет обнаружить проблему (MTTD <= 2 мин) и диагностировать ее (MTTR <= 15 мин).

Текущий мониторинг (Prometheus + Grafana + Loki + Alertmanager) покрывает базовые инфраструктурные метрики, но не обеспечивает наблюдаемость event-driven архитектуры: consumer lag, end-to-end latency событий, состояние саг, throughput Flink-джобов.

## Требования

Определить подход к мониторингу событийной платформы, список метрик, инструменты и их интеграцию в архитектуру.

## Решение

### Подход к мониторингу: три столпа наблюдаемости

Мониторинг событийной платформы строится на трех столпах наблюдаемости:

1. Метрики (Metrics) - числовые показатели, агрегированные во временные ряды. Отвечают на вопрос "что происходит?" (throughput, latency, error rate).
2. Трейсы (Traces) - сквозная трассировка одного запроса/события через все сервисы. Отвечает на вопрос "где тормозит?".
3. Логи (Logs) - детальная информация о событиях. Отвечает на вопрос "почему произошло?".

Обоснование выбора подхода:

Event-driven архитектура принципиально отличается от синхронной request-response модели:

- Запрос проходит через асинхронные этапы (produce -> broker -> consume -> process), каждый из которых может стать узким местом.
- Saga растягивается на секунды, проходя через 5-7 сервисов - нужно отслеживать каждый шаг.
- Consumer lag - ключевая метрика: если consumer отстает, пользователь видит задержку.
- Flink-джобы имеют собственный lifecycle и метрики (checkpoint, backpressure).

Поэтому помимо стандартных RED-метрик (Rate, Errors, Duration) для каждого сервиса, необходим отдельный слой метрик для событийной инфраструктуры.

### Выбранные инструменты мониторинга

| Инструмент              | Роль                   | Обоснование                                                                                                                 |
| ----------------------- | ---------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| Prometheus              | Сбор и хранение метрик | Уже используется; де-факто стандарт для Kubernetes; поддерживает PromQL для алертинга                                       |
| Grafana                 | Визуализация           | Уже используется; дашборды для метрик, логов и трейсов в едином UI                                                          |
| Grafana Tempo           | Distributed tracing    | Интеграция с Grafana stack; поддержка OpenTelemetry; горизонтальное масштабирование; экономичнее Jaeger для больших объемов |
| Grafana Loki            | Централизованные логи  | Уже используется; label-based индексация (экономичнее Elasticsearch для логов)                                              |
| Alertmanager            | Алертинг               | Уже используется; маршрутизация, группировка, silencing                                                                     |
| OpenTelemetry Collector | Сбор телеметрии        | Единый агент для метрик, трейсов и логов; vendor-agnostic; стандарт CNCF                                                    |
| Kafka Exporter          | Метрики Kafka          | Экспорт consumer lag, partition metrics в Prometheus                                                                        |
| Flink Metrics Reporter  | Метрики Flink          | Встроенный Prometheus reporter для Flink-джобов                                                                             |
| PagerDuty / OpsGenie    | Incident management    | Эскалация алертов, on-call ротация, SLA tracking                                                                            |

### Архитектура сбора телеметрии

Диаграмма: [diagrams/observability-architecture.puml](diagrams/observability-architecture.puml)

Correlation: OpenTelemetry SDK инъектирует `trace_id` в каждое Kafka-сообщение (header `traceparent`). Consumer извлекает `trace_id` и продолжает трейс. Это обеспечивает сквозную трассировку: HTTP-запрос -> Kafka produce -> Kafka consume -> обработка -> следующий produce.

### Список метрик

#### 1. Метрики Kafka-кластера

| Метрика                                | Тип       | Описание                                | Алерт (порог)       |
| -------------------------------------- | --------- | --------------------------------------- | ------------------- |
| `kafka_broker_count`                   | Gauge     | Количество живых брокеров               | < 3 (P1)            |
| `kafka_controller_active_count`        | Gauge     | Активный контроллер                     | ≠ 1 (P1)            |
| `kafka_under_replicated_partitions`    | Gauge     | Партиции с недостаточной репликацией    | > 0 (P1)            |
| `kafka_offline_partitions_count`       | Gauge     | Недоступные партиции                    | > 0 (P1)            |
| `kafka_topic_partition_current_offset` | Counter   | Текущий offset партиции                 | - (для расчета lag) |
| `kafka_log_size_bytes`                 | Gauge     | Размер логов на диске                   | > 80% диска (P2)    |
| `kafka_network_request_rate`           | Rate      | RPS на брокер                           | > 80% capacity (P2) |
| `kafka_request_latency_ms`             | Histogram | Латентность обработки запросов брокером | p99 > 100ms (P2)    |
| `kafka_isr_shrink_rate`                | Rate      | Частота уменьшения ISR                  | > 0 за 5 мин (P2)   |

#### 2. Метрики Consumer

| Метрика                             | Тип       | Описание                                 | Алерт (порог)             |
| ----------------------------------- | --------- | ---------------------------------------- | ------------------------- |
| `kafka_consumer_group_lag`          | Gauge     | Отставание consumer group (по партициям) | > 10K (P2), > 100K (P1)   |
| `kafka_consumer_group_lag_seconds`  | Gauge     | Отставание в секундах (time-based lag)   | > 30s (P2), > 300s (P1)   |
| `event_processing_duration_seconds` | Histogram | Время обработки одного события           | p99 > 1s (P2)             |
| `event_processing_errors_total`     | Counter   | Количество ошибок обработки              | rate > 1/min (P2)         |
| `event_processing_rate`             | Rate      | Скорость обработки событий/сек           | < 50% от нормального (P2) |
| `consumer_rebalance_total`          | Counter   | Количество ребалансировок                | > 5/час (P3)              |
| `consumer_commit_latency_ms`        | Histogram | Латентность commit offset                | p99 > 500ms (P3)          |

#### 3. Метрики Producer (Outbox)

| Метрика                             | Тип     | Описание                                   | Алерт (порог)             |
| ----------------------------------- | ------- | ------------------------------------------ | ------------------------- |
| `outbox_pending_count`              | Gauge   | Количество неотправленных событий в outbox | > 1000 (P2)               |
| `outbox_oldest_pending_age_seconds` | Gauge   | Возраст старейшего неотправленного события | > 30s (P1)                |
| `outbox_publish_rate`               | Rate    | Скорость публикации из outbox              | < 50% от нормального (P2) |
| `outbox_publish_errors_total`       | Counter | Ошибки публикации                          | rate > 1/min (P2)         |
| `kafka_producer_record_send_rate`   | Rate    | RPS отправки сообщений                     | - (baseline)              |
| `kafka_producer_record_error_rate`  | Rate    | Частота ошибок отправки                    | > 0.1% (P2)               |

#### 4. Метрики Saga

| Метрика                      | Тип       | Описание                                  | Алерт (порог)               |
| ---------------------------- | --------- | ----------------------------------------- | --------------------------- |
| `saga_active_count`          | Gauge     | Количество активных саг (по типу)         | > 50K booking saga (P2)     |
| `saga_completed_total`       | Counter   | Завершенные саги                          | - (для rate)                |
| `saga_compensated_total`     | Counter   | Саги, потребовавшие компенсации           | rate > 5% от completed (P2) |
| `saga_failed_total`          | Counter   | Окончательно провалившиеся саги           | > 0/мин (P1)                |
| `saga_duration_seconds`      | Histogram | Длительность саги от начала до завершения | p99 > 30s (P2) для booking  |
| `saga_step_duration_seconds` | Histogram | Длительность каждого шага (label: step)   | p99 > 5s (P2)               |
| `saga_timeout_total`         | Counter   | Саги, превысившие таймаут                 | > 0/мин (P2)                |
| `saga_step_retry_total`      | Counter   | Повторные попытки шагов                   | rate > 10% (P3)             |

#### 5. Метрики Flink

| Метрика                                         | Тип       | Описание                               | Алерт (порог)        |
| ----------------------------------------------- | --------- | -------------------------------------- | -------------------- |
| `flink_job_status`                              | Gauge     | Статус джоба (RUNNING=1, FAILED=0)     | ≠ 1 (P1)             |
| `flink_taskmanager_numRegistered`               | Gauge     | Количество TaskManager                 | < expected (P1)      |
| `flink_job_numRecordsInPerSecond`               | Rate      | Входящий throughput                    | < 50% baseline (P2)  |
| `flink_job_numRecordsOutPerSecond`              | Rate      | Исходящий throughput                   | < 50% baseline (P2)  |
| `flink_job_latency_source_to_sink_ms`           | Histogram | Латентность обработки (source -> sink) | p99 > 5s (P1)        |
| `flink_job_lastCheckpointDuration`              | Gauge     | Длительность последнего checkpoint     | > 60s (P2)           |
| `flink_job_lastCheckpointSize`                  | Gauge     | Размер последнего checkpoint           | > 10GB (P3)          |
| `flink_job_numberOfFailedCheckpoints`           | Counter   | Количество провалившихся checkpoints   | > 3 подряд (P1)      |
| `flink_taskmanager_Status_JVM_Memory_Heap_Used` | Gauge     | Использование heap памяти              | > 85% (P2)           |
| `flink_job_isBackPressured`                     | Gauge     | Наличие backpressure                   | = 1 более 5 мин (P2) |

#### 6. Метрики DLQ

| Метрика                        | Тип     | Описание                                         | Алерт (порог)                                      |
| ------------------------------ | ------- | ------------------------------------------------ | -------------------------------------------------- |
| `dlq_messages_total`           | Counter | Сообщения, попавшие в DLQ (label: topic)         | > 0 за 5 мин (P2 для payment, P3 для notification) |
| `dlq_messages_pending`         | Gauge   | Необработанные сообщения в DLQ                   | > 100 (P2)                                         |
| `dlq_oldest_message_age_hours` | Gauge   | Возраст старейшего необработанного DLQ-сообщения | > 24h (P2)                                         |
| `dlq_reprocessed_total`        | Counter | Переобработанные DLQ-сообщения                   | - (tracking)                                       |

#### 7. Бизнес-метрики (через события)

| Метрика                                | Тип       | Описание                                  | Алерт (порог)                               |
| -------------------------------------- | --------- | ----------------------------------------- | ------------------------------------------- |
| `bookings_created_rate`                | Rate      | Заказы в минуту                           | < 50% от нормального для времени суток (P2) |
| `bookings_confirmed_rate`              | Rate      | Подтвержденные заказы в минуту            | < 50% от created (P1)                       |
| `booking_confirmation_latency_seconds` | Histogram | Время от создания до подтверждения заказа | p95 > 30s (P2)                              |
| `active_trips_count`                   | Gauge     | Активные поездки                          | > 500K (P2, приближение к лимиту)           |
| `payment_success_rate`                 | Rate      | Доля успешных платежей                    | < 95% (P1)                                  |
| `payment_processing_duration_seconds`  | Histogram | Время обработки платежа                   | p95 > 5s (P2)                               |
| `driver_matching_duration_seconds`     | Histogram | Время подбора водителя                    | p95 > 10s (P2)                              |
| `driver_online_count`                  | Gauge     | Водители на линии (по региону)            | < expected (P3)                             |
| `surge_coefficient_by_zone`            | Gauge     | Текущий surge-коэффициент (по зонам)      | > 5.0 (P3, аномалия)                        |
| `notification_delivery_rate`           | Rate      | Доля доставленных уведомлений             | < 90% (P3)                                  |

#### 8. End-to-End метрики

| Метрика                                   | Тип       | Описание                                  | Алерт (порог)                       |
| ----------------------------------------- | --------- | ----------------------------------------- | ----------------------------------- |
| `e2e_booking_to_payment_duration_seconds` | Histogram | Время от заказа до оплаты                 | p95 > 60s (P3, baseline monitoring) |
| `e2e_event_propagation_delay_seconds`     | Histogram | Задержка от produce до финального consume | p99 > 5s (P2)                       |
| `e2e_saga_success_rate`                   | Rate      | Доля успешно завершенных саг (по типу)    | < 95% для booking (P1)              |

### Дашборды Grafana

| Дашборд                 | Описание                               | Ключевые панели                                                                              |
| ----------------------- | -------------------------------------- | -------------------------------------------------------------------------------------------- |
| Event Platform Overview | Общее состояние событийной платформы   | Throughput всех топиков, общий consumer lag, DLQ-сообщения, активные саги                    |
| Kafka Cluster Health    | Состояние Kafka-кластеров (per region) | Broker status, ISR, under-replicated partitions, disk usage, network I/O                     |
| Consumer Groups         | Состояние consumer groups              | Lag per group, processing rate, error rate, rebalance frequency                              |
| Saga Monitor            | Состояние саг                          | Active/completed/failed саги, duration histogram, step duration breakdown, compensation rate |
| Flink Jobs              | Состояние Flink-джобов                 | Job status, throughput in/out, latency, checkpoint duration, backpressure, memory            |
| Booking Flow            | Бизнес-метрики бронирования            | Bookings/min, confirmation rate, matching duration, cancellation reasons                     |
| Payment Flow            | Бизнес-метрики платежей                | Payment success rate, processing time, refund rate, payout status                            |
| DLQ Dashboard           | Управление DLQ                         | DLQ messages per topic, age, reprocessing status                                             |

### Алертинг: уровни severity

| Severity | Описание                                          | Канал                                  | Время реакции | Пример                                                        |
| -------- | ------------------------------------------------- | -------------------------------------- | ------------- | ------------------------------------------------------------- |
| P1       | Потеря данных, недоступность критического сервиса | PagerDuty (on-call) + Slack #incidents | < 5 мин       | Kafka offline partitions, Flink job failed, saga_failed > 0   |
| P2       | Деградация производительности, рост ошибок        | Slack #alerts                          | < 15 мин      | Consumer lag > 10K, outbox pending > 30s, error rate > 1%     |
| P3       | Предупреждение, требует внимания                  | Slack #warnings                        | < 1 час       | DLQ notification messages, surge anomaly, high rebalance rate |

## Альтернативы

1. Jaeger вместо Tempo - более зрелый проект, больше UI возможностей, но не интегрирован нативно с Grafana stack; требует отдельного хранилища (Elasticsearch/Cassandra), что увеличивает операционные затраты.
2. ELK (Elasticsearch + Logstash + Kibana) вместо Loki - более мощный full-text search, но значительно дороже в ресурсах (Elasticsearch требует ~10x больше памяти), и не интегрирован с Grafana.
3. Datadog / New Relic (SaaS) - меньше операционных затрат (managed), но дороже при масштабе GoFuture (~150K events/s), lock-in на вендора, данные уходят во внешний сервис (compliance concern для финансовых данных).
4. VictoriaMetrics вместо Prometheus - лучше масштабируется для хранения метрик, но Prometheus остается стандартом для Kubernetes ecosystem и уже развернут.

## Компромиссы

- OpenTelemetry SDK добавляет ~1-2% overhead к каждому сервису (CPU/memory). Для driver.location.updates (100K/s) - sampling rate 1% для трейсов (каждый 100-й запрос), 100% для метрик.
- 100% tracing для критического пути (booking, payment) генерирует значительный объем данных. Tempo с S3-backend для холодного хранения и retention 7 дней для hot-данных.
- Бизнес-метрики через события создают зависимость мониторинга от Kafka. Митигация: базовые health-метрики сервисов собираются напрямую через Prometheus scrape, не через Kafka.
- PagerDuty - платный сервис. Альтернатива: Alertmanager -> Telegram/Slack bot для on-call ротации (менее надежно, но бесплатно).
- Множество дашбордов требует поддержки (Grafana as Code через provisioning). Митигация: дашборды хранятся в Git, деплоятся через CI/CD.
