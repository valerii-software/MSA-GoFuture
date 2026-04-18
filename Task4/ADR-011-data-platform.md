# ADR-011: Единая платформа данных для ML и BI

## Контекст

GoFuture использует три ML-модели, критичные для бизнеса:

- Динамическое ценообразование (Surge Pricing) - определяет коэффициент повышенного спроса в реальном времени.
- Прогнозирование спроса (Demand Forecasting) - предсказывает число заказов по геозонам на 15-60 мин вперед.
- Обнаружение мошенничества (Fraud Detection) - оценивает риск каждого заказа и платежа.

Эти модели требуют данных из всех микросервисов (Booking, Driver, Pricing, Payments, Geography, Fraud), как в реальном времени (для inference), так и исторических (для обучения). Кроме того, бизнесу необходимы BI-отчеты по всем аспектам платформы.

Текущее состояние:

- Данные разрозненны: каждый сервис владеет своей БД (Database per Service).
- Flink-джобы (ADR-006) обрабатывают потоки событий, но нет единого хранилища для обучения моделей.
- ClickHouse используется для аналитики, но без формализованного ETL и контроля качества.
- Нет Feature Store - фичи вычисляются ad-hoc в каждой модели.

## Решение

### 1. Архитектура пайплайна данных

Диаграмма: [C4-data-pipeline.puml](C4-data-pipeline.puml)

Архитектура построена по принципу Kappa+ (stream-first с batch-дополнением):

- Stream-слой (Flink) - основной путь данных для real-time inference и агрегации. Все события проходят через Kafka -> Flink -> Feature Store / ClickHouse.
- Batch-слой (Spark) - дополнительный путь для тяжелых вычислений: обучение моделей, исторические агрегации, backfill.
- Data Lake (S3 + Apache Iceberg) - единое хранилище сырых и обработанных данных для обучения и аналитики.

Почему Kappa+, а не Lambda:

- Lambda требует дублирования логики в stream и batch, что усложняет поддержку.
- В GoFuture stream (Flink) уже обрабатывает основные потоки (ADR-006).
- Spark используется только для задач, где stream неэффективен (обучение ML, тяжелые JOIN-ы по историческим данным).

### 2. Слой сбора данных (Data Ingestion)

Данные поступают из трех источников:

| Источник                 | Механизм                      | Примеры данных                                                         | Объем                     |
| ------------------------ | ----------------------------- | ---------------------------------------------------------------------- | ------------------------- |
| Доменные события (Kafka) | Уже существует (ADR-006)      | BookingCreated, TripCompleted, PaymentCompleted, DriverLocationUpdated | ~150K событий/сек на пике |
| CDC (Debezium)           | Уже существует (ADR-008)      | Изменения в operational DBs (bookings, drivers, payments)              | ~10K changes/сек          |
| Внешние источники        | API-интеграции + Batch ingest | Погодные данные, городские события, карты дорог                        | ~100 запросов/мин         |

Принцип: все данные проходят через Kafka. Это обеспечивает единую точку подключения для всех потребителей (Flink, Spark, ClickHouse, ML).

### 3. Слой обработки (Data Processing)

#### 3.1 Stream Processing (Apache Flink)

Flink-джобы, уже описанные в ADR-006, дополняются новыми джобами для ML-платформы:

| Джоб                                           | Вход                                              | Выход                            | Назначение                                                                                  | Latency  |
| ---------------------------------------------- | ------------------------------------------------- | -------------------------------- | ------------------------------------------------------------------------------------------- | -------- |
| GeoIndexer (существующий)                      | driver.location.updates                           | Elasticsearch + GeozoneEvents    | Геоиндексация водителей                                                                     | < 1 с    |
| Surge Pricing (существующий)                   | driver.location + booking.lifecycle + zone.events | SurgeUpdated + Redis             | Расчет surge в реальном времени                                                             | < 5 с    |
| Fraud Detection (существующий)                 | booking + payment + location events               | FraudDetected + FraudCheckPassed | Real-time risk scoring                                                                      | < 200 мс |
| Feature Aggregator (новый)                     | Все бизнес-события                                | Feature Store (online)           | Агрегация real-time фичей для ML: bookings/min по зоне, avg ETA, driver supply/demand ratio | < 5 с    |
| Data Lake Writer (новый)                       | Все бизнес-события                                | S3 (Apache Iceberg)              | Запись сырых событий в Data Lake (partitioned by date + event_type)                         | < 30 с   |
| ClickHouse Writer (существующий, доработанный) | Агрегированные метрики                            | ClickHouse                       | BI-ready агрегации: hourly bookings, revenue, conversion                                    | < 60 с   |

#### 3.2 Batch Processing (Apache Spark)

Spark-джобы выполняются по расписанию:

| Джоб                  | Расписание               | Вход                              | Выход                         | Назначение                                          |
| --------------------- | ------------------------ | --------------------------------- | ----------------------------- | --------------------------------------------------- |
| Training Data Builder | Ежедневно, 03:00 UTC     | Iceberg (raw events)              | Iceberg (training datasets)   | Построение обучающих выборок для всех ML-моделей    |
| Feature Backfill      | По требованию            | Iceberg (raw events)              | Feature Store (offline)       | Пересчет исторических фичей при добавлении новых    |
| Historical Aggregator | Ежедневно, 04:00 UTC     | Iceberg (raw events)              | ClickHouse                    | Тяжелые агрегации: когортный анализ, retention, LTV |
| Data Quality Checks   | Каждые 6 часов           | Iceberg + ClickHouse              | Quality metrics -> Prometheus | Проверка полноты, свежести, корректности данных     |
| Model Retraining      | По расписанию (см. ниже) | Training datasets + Feature Store | MLflow Model Registry         | Обучение и валидация ML-моделей                     |

### 4. Слой хранения (Data Storage)

#### 4.1 Data Lake (S3 + Apache Iceberg)

```
s3://gofuture-data-lake/
    raw/                          # Сырые события (as-is из Kafka)
│   ├── booking.trip.lifecycle/   # Partitioned by date
│   │   ├── dt=2026-04-06/
│   │   └── ...
│   ├── driver.location.updates/  # Sampled: каждое 10-е событие
│   ├── payment.transaction/
│   └── ...
├── processed/                    # Очищенные и обогащенные данные
│   ├── trips_enriched/           # Trip + pricing + geography + driver
│   ├── payments_enriched/        # Payment + booking + fraud_score
│   └── driver_sessions/          # Сессии водителей (online -> offline)
├── features/                     # Offline Feature Store
│   ├── user_features/            # Агрегации по пользователям
│   ├── driver_features/          # Агрегации по водителям
│   ├── zone_features/            # Агрегации по геозонам
│   └── temporal_features/        # Временные паттерны
└── training/                     # Обучающие выборки
    ├── surge_model_v3/
    ├── demand_model_v5/
    └── fraud_model_v7/
```

Почему Apache Iceberg:

- ACID-транзакции на S3 (concurrent writes от Flink и Spark).
- Time travel - можно читать данные на любой момент в прошлом (важно для reproducibility обучения).
- Schema evolution - добавление новых полей без перезаписи данных.
- Partition evolution - изменение схемы партиционирования без перезаписи.
- Совместимость с Flink и Spark (единый формат для stream и batch).

Retention:

- `raw/`: 90 дней (после - архив в S3 Glacier).
- `processed/`: 1 год.
- `training/`: бессрочно (версионированы).

#### 4.2 Feature Store (Feast)

Feature Store - центральное хранилище ML-фичей, обеспечивающее:

- Единообразие: одни и те же фичи используются при обучении и inference (train-serve skew elimination).
- Переиспользование: фичи, вычисленные для Surge, доступны для Fraud Detection.
- Versioning: изменение логики вычисления фичи не ломает старые модели.

| Слой          | Технология          | Назначение                          | Latency |
| ------------- | ------------------- | ----------------------------------- | ------- |
| Online Store  | Redis               | Свежие фичи для real-time inference | < 5 мс  |
| Offline Store | Apache Iceberg (S3) | Исторические фичи для обучения      | секунды |

Примеры фичей:

| Фича                              | Тип               | Обновление                          | Используется в          |
| --------------------------------- | ----------------- | ----------------------------------- | ----------------------- |
| `zone_bookings_last_15min`        | Online            | Flink (каждые 5 сек)                | Surge, Demand           |
| `zone_active_drivers`             | Online            | Flink (каждые 5 сек)                | Surge, Demand, Matching |
| `zone_supply_demand_ratio`        | Online            | Flink (каждые 5 сек)                | Surge                   |
| `user_trips_last_30d`             | Offline -> Online | Spark (daily), materialized в Redis | Fraud                   |
| `user_avg_trip_price`             | Offline -> Online | Spark (daily)                       | Fraud, Pricing          |
| `user_cancellation_rate`          | Offline -> Online | Spark (daily)                       | Fraud, Matching         |
| `driver_rating_30d`               | Offline -> Online | Spark (daily)                       | Matching                |
| `driver_acceptance_rate`          | Offline -> Online | Spark (daily)                       | Matching                |
| `zone_avg_surge_7d`               | Offline           | Spark (daily)                       | Demand                  |
| `zone_demand_by_hour_dow`         | Offline           | Spark (weekly)                      | Demand                  |
| `payment_velocity_1h`             | Online            | Flink (каждые 30 сек)               | Fraud                   |
| `user_device_fingerprint_changes` | Online            | Flink (event-driven)                | Fraud                   |

#### 4.3 Analytical DB (ClickHouse)

ClickHouse остается основным хранилищем для BI-запросов. Данные поступают двумя путями:

- Stream: Flink -> ClickHouse (materialized views, latency < 60 сек).
- Batch: Spark -> ClickHouse (тяжелые агрегации, daily).

Ключевые таблицы ClickHouse:

| Таблица                | Источник             | Описание                                                | Retention           |
| ---------------------- | -------------------- | ------------------------------------------------------- | ------------------- |
| `trips_raw`            | Flink (stream)       | Все поездки с обогащением (driver, pricing, geography)  | 1 год               |
| `trips_hourly`         | Materialized View    | Почасовые агрегации: count, revenue, avg_price, avg_eta | 3 года              |
| `payments_raw`         | Flink (stream)       | Все платежи                                             | 3 года (compliance) |
| `driver_sessions`      | Flink (stream)       | Сессии водителей (online/offline, earnings)             | 1 год               |
| `surge_history`        | Flink (stream)       | Surge коэффициенты по зонам во времени                  | 1 год               |
| `fraud_events`         | Flink (stream)       | Все fraud-проверки и решения                            | 3 года              |
| `cohort_analysis`      | Spark (batch, daily) | Когортные метрики: retention, LTV, churn                | 3 года              |
| `zone_demand_forecast` | ML pipeline (batch)  | Прогнозы спроса по зонам                                | 90 дней             |

### 5. ML-платформа

#### 5.1 Компоненты

| Компонент           | Технология            | Назначение                                                 |
| ------------------- | --------------------- | ---------------------------------------------------------- |
| Feature Store       | Feast                 | Online + Offline хранение фичей                            |
| Experiment Tracking | MLflow Tracking       | Логирование экспериментов, метрик, параметров              |
| Model Registry      | MLflow Model Registry | Версионирование моделей, стадии (Staging -> Production)    |
| Model Training      | Spark MLlib + PyTorch | Обучение на данных из Iceberg + Feature Store              |
| Model Serving       | KServe (Kubernetes)   | Real-time inference с автоскейлингом                       |
| Model Monitoring    | Evidently AI          | Мониторинг data drift, prediction drift, model performance |
| Orchestration       | Apache Airflow        | Оркестрация pipeline: training -> validation -> deployment |

#### 5.2 ML-модели

Модель 1: Surge Pricing (динамическое ценообразование)

| Параметр        | Значение                                                                                                    |
| --------------- | ----------------------------------------------------------------------------------------------------------- |
| Тип             | Gradient Boosted Trees (LightGBM)                                                                           |
| Inference       | Real-time через KServe (< 50 мс)                                                                            |
| Фичи (online)   | zone_bookings_last_15min, zone_active_drivers, supply_demand_ratio, current_surge, time_of_day, day_of_week |
| Фичи (offline)  | zone_avg_surge_7d, zone_historical_demand, weather_forecast, city_events                                    |
| Обучение        | Ежедневно (03:00 UTC) на Spark                                                                              |
| Данные обучения | 30 дней trips + surge_history из Iceberg                                                                    |
| Валидация       | MAE < 0.15 (surge coefficient), A/B-тест 5% трафика перед production                                        |
| Fallback        | При недоступности модели - rule-based surge (supply/demand ratio)                                           |

Модель 2: Demand Forecasting (прогнозирование спроса)

| Параметр        | Значение                                                                                        |
| --------------- | ----------------------------------------------------------------------------------------------- |
| Тип             | Temporal Fusion Transformer (PyTorch)                                                           |
| Inference       | Batch каждые 15 мин (Airflow -> Spark -> Feature Store)                                         |
| Фичи            | zone_demand_by_hour_dow, zone_bookings_trend, weather, city_events, holidays, zone_avg_surge_7d |
| Обучение        | Еженедельно (воскресенье, 02:00 UTC)                                                            |
| Данные обучения | 90 дней trips из Iceberg, агрегированных по zone + 15min window                                 |
| Валидация       | MAPE < 15%, сравнение с baseline (seasonal naive)                                               |
| Потребители     | Surge Pricing (input), Smart Matching (pre-positioning), BI dashboards                          |

Модель 3: Fraud Detection (обнаружение мошенничества)

| Параметр        | Значение                                                                                      |
| --------------- | --------------------------------------------------------------------------------------------- |
| Тип             | Ensemble: LightGBM + Neural Network (PyTorch)                                                 |
| Inference       | Real-time через KServe (< 100 мс)                                                             |
| Фичи (online)   | payment_velocity_1h, device_fingerprint_changes, user_location_anomaly, booking_pattern_score |
| Фичи (offline)  | user_trips_last_30d, user_cancellation_rate, user_avg_trip_price, user_chargeback_history     |
| Обучение        | Еженедельно (среда, 02:00 UTC)                                                                |
| Данные обучения | 90 дней fraud_events + payments из Iceberg                                                    |
| Валидация       | Precision > 95% при Recall > 80%, F1 > 0.87                                                   |
| Fallback        | При недоступности - rule-based scoring (blacklists + velocity rules)                          |

#### 5.3 ML Pipeline (Airflow DAG)

```
[Schedule / Trigger]
[1. Data Extraction]     <- Iceberg -> training dataset
[2. Feature Engineering] <- Feature Store (offline) + raw data
[3. Data Validation]     <- Great Expectations: schema + distribution checks
[4. Model Training]      <- Spark MLlib / PyTorch
[5. Model Validation]    <- Сравнение с baseline + текущей production моделью
  PASS -> [6. Register in MLflow (stage: Staging)]
          [7. Shadow Deployment] <- 5% трафика, сравнение предсказаний
              PASS -> [8. Promote to Production]
              FAIL -> [Alert: Model degradation]
  FAIL -> [Alert: Training failed, keep current model]
```

Диаграмма: [diagrams/data-quality-pipeline.puml](diagrams/data-quality-pipeline.puml)

### 6. BI-инструменты

| Инструмент  | Источник                     | Аудитория                   | Назначение                                                                  |
| ----------- | ---------------------------- | --------------------------- | --------------------------------------------------------------------------- |
| DataLens    | ClickHouse                   | Бизнес-менеджеры, аналитики | Интерактивные дашборды: unit-экономика, воронки, когорты                    |
| Grafana     | Prometheus + ClickHouse      | SRE, DevOps                 | Операционные метрики: throughput, latency, error rate, ML model performance |
| Jupyter Hub | Iceberg (S3) + Feature Store | Data Scientists             | Exploratory analysis, ad-hoc запросы, прототипирование моделей              |
| Airflow UI  | Airflow metadata DB          | Data Engineers              | Мониторинг DAG-ов: статус, длительность, ошибки                             |

Ключевые BI-дашборды:

| Дашборд              | Платформа          | Метрики                                                                           |
| -------------------- | ------------------ | --------------------------------------------------------------------------------- |
| Executive Overview   | DataLens           | GMV, revenue, active users, trips/day, avg price, NPS                             |
| Regional Performance | DataLens           | Метрики по регионам (SGP, JKT, GRU, FRA), сравнение                               |
| Unit Economics       | DataLens           | CAC, LTV, take rate, driver earnings, cost per trip                               |
| Demand & Supply      | DataLens + Grafana | Supply/demand ratio по зонам, прогноз vs факт, surge distribution                 |
| Fraud Analytics      | DataLens           | Fraud rate, false positive rate, blocked amount, fraud by category                |
| ML Model Health      | Grafana            | Prediction latency, drift score, feature freshness, model version, A/B results    |
| Data Platform Health | Grafana            | Pipeline lag, Flink throughput, Spark job duration, data freshness, quality score |

### 7. Обеспечение качества данных (Data Quality)

Диаграмма: [diagrams/data-quality-pipeline.puml](diagrams/data-quality-pipeline.puml)

Качество данных критично для GoFuture: некорректные данные -> плохие ML-модели -> неправильный surge -> потеря водителей или пассажиров. Стратегия контроля качества охватывает весь жизненный цикл данных.

#### 7.1 Уровень 1: Schema Validation (на входе)

Механизм: Confluent Schema Registry + Avro (уже реализовано в ADR-006).

- Каждое событие в Kafka валидируется против зарегистрированной Avro-схемы.
- Backward-compatible эволюция: новые поля - опциональные, удаление - только через новую версию топика.
- Невалидные события отклоняются producer-ом (серьезные) или направляются в DLQ (мягкие ошибки).

Что проверяется:

- Типы полей (string, int, float, timestamp).
- Обязательные поля (NOT NULL).
- Enum-значения (status IN ('PENDING', 'COMPLETED', 'CANCELLED')).

Чего недостаточно: Schema валидирует структуру, но не семантику. `price: -100.0` пройдет валидацию (float), но семантически некорректно.

#### 7.2 Уровень 2: Data Contracts (между командами)

Механизм: Формализованные контракты между producer-ами и consumer-ами данных.

Data Contract - YAML-документ, описывающий:

```yaml
# contracts/booking.trip.lifecycle.v1.yaml
contract:
  name: "booking.trip.lifecycle.v1"
  owner: "booking-team"
  version: "1.3.0"
  sla:
    freshness: "< 30 seconds" # Максимальная задержка от события до Kafka
    completeness: "> 99.9%" # Доля событий без пропущенных полей
    availability: "99.95%" # Uptime producer-а
  schema:
    type: "avro"
    registry: "schema-registry.gofuture.internal"
  semantic_rules:
    - field: "estimated_price"
      rule: "value > 0 AND value < 100000"
      severity: "error"
    - field: "pickup_location.lat"
      rule: "value >= -90 AND value <= 90"
      severity: "error"
    - field: "created_at"
      rule: "value <= NOW() + 5min" # Не из будущего
      severity: "warning"
  consumers:
    - name: "analytics-service"
      usage: "BI reports"
    - name: "fraud-detection-flink"
      usage: "Real-time fraud scoring"
    - name: "data-lake-writer-flink"
      usage: "Raw data archival"
```

Процесс:

- Контракты хранятся в Git (monorepo `data-contracts/`).
- CI/CD проверяет контракт при изменении producer-а: если breaking change - блокировка деплоя.
- Consumer-ы подписываются на контракт, получают уведомление о deprecation.

#### 7.3 Уровень 3: Automated Quality Checks (Great Expectations)

Механизм: Great Expectations (GX) - framework для декларативных проверок качества данных.

Где выполняется:

- Stream (Flink): легкие проверки в реальном времени (range checks, null checks).
- Batch (Spark): полные проверки каждые 6 часов на данных в Iceberg.

Типы проверок:

| Категория    | Примеры проверок                                        | Severity | Действие при нарушении                        |
| ------------ | ------------------------------------------------------- | -------- | --------------------------------------------- |
| Completeness | % NULL в обязательных полях < 0.1%                      | Error    | Alert + блокировка downstream pipeline        |
| Freshness    | Разница между event_time и processing_time < 5 мин      | Warning  | Alert в Slack                                 |
| Validity     | price > 0, lat ∈ [-90, 90], status ∈ enum               | Error    | Событие -> DLQ                                |
| Uniqueness   | Уникальность event_id, booking_id                       | Warning  | Дедупликация (idempotent consumer)            |
| Consistency  | sum(payments) ≈ sum(trip_prices) (±1%)                  | Error    | Alert + расследование                         |
| Distribution | avg(price) не отклоняется > 3σ от 7-дневного среднего   | Warning  | Alert (возможная аномалия данных или бизнеса) |
| Volume       | Количество событий/час не отклоняется > 50% от baseline | Warning  | Alert (возможный сбой producer-а)             |
| Timeliness   | Обучающие данные обновлены < 24 часов назад             | Error    | Блокировка model retraining                   |

Пример GX Expectation Suite:

```python
# expectations/trips_enriched.py
expectation_suite = {
    "expectation_suite_name": "trips_enriched_quality",
    "expectations": [
        # Completeness
        {"expectation_type": "expect_column_values_to_not_be_null",
         "kwargs": {"column": "booking_id"}},
        {"expectation_type": "expect_column_values_to_not_be_null",
         "kwargs": {"column": "driver_id"}},

        # Validity
        {"expectation_type": "expect_column_values_to_be_between",
         "kwargs": {"column": "price", "min_value": 0, "max_value": 100000}},
        {"expectation_type": "expect_column_values_to_be_between",
         "kwargs": {"column": "pickup_lat", "min_value": -90, "max_value": 90}},

        # Distribution
        {"expectation_type": "expect_column_mean_to_be_between",
         "kwargs": {"column": "price", "min_value": 100, "max_value": 5000}},

        # Volume
        {"expectation_type": "expect_table_row_count_to_be_between",
         "kwargs": {"min_value": 10000, "max_value": 10000000}},  # per day

        # Freshness
        {"expectation_type": "expect_column_max_to_be_between",
         "kwargs": {"column": "created_at",
                    "min_value": "now() - interval '6 hours'",
                    "max_value": "now()"}}
    ]
}
```

#### 7.4 Уровень 4: Data Lineage (OpenLineage)

Механизм: OpenLineage - открытый стандарт для отслеживания происхождения данных.

Что отслеживается:

- Откуда пришли данные (Kafka topic -> Flink job -> Iceberg table -> Spark job -> ClickHouse).
- Какие трансформации были применены (filter, join, aggregate, enrich).
- Кто потребляет данные (ML-модель, BI-дашборд, другой pipeline).

Зачем:

- Root Cause Analysis: если BI-отчет показывает аномалию - можно проследить цепочку до исходного события и найти, где произошла ошибка.
- Impact Analysis: если меняется схема события BookingCreated - видно, какие pipeline-ы, таблицы и модели затронуты.
- Compliance: аудиторы могут проверить, как именно обрабатываются персональные данные.

Интеграция:

- Flink и Spark отправляют lineage-события в OpenLineage server.
- Визуализация: Marquez UI (open-source lineage explorer).
- Связь с Data Contracts: lineage автоматически проверяет, что consumer получает данные из source, указанного в контракте.

#### 7.5 Уровень 5: ML Data Quality (Evidently AI)

Специфичные проверки для ML-данных:

| Проверка          | Описание                                                      | Частота                  | Действие                    |
| ----------------- | ------------------------------------------------------------- | ------------------------ | --------------------------- |
| Feature Drift     | Распределение фичей в production отличается от training       | Ежечасно                 | Alert -> ускоренный retrain |
| Prediction Drift  | Распределение предсказаний модели изменилось                  | Ежечасно                 | Alert -> расследование      |
| Label Drift       | Распределение целевой переменной (fraud/not fraud) изменилось | Ежедневно                | Alert -> пересмотр модели   |
| Feature Freshness | Фича в Feature Store устарела (online store)                  | Каждые 5 мин             | Alert -> проверка Flink job |
| Train-Serve Skew  | Фичи при inference отличаются от фичей при обучении           | При каждом деплое модели | Блокировка деплоя           |

Метрики качества данных (Prometheus):

```
# Общие метрики DQ
data_quality_score{dataset="trips_enriched"}        0.97   # Общий DQ score (0-1)
data_quality_completeness{dataset="trips_enriched"}  0.999  # % non-null обязательных полей
data_quality_freshness_seconds{dataset="trips_raw"}  25     # Возраст самой свежей записи
data_quality_validity{dataset="payments_raw"}        0.998  # % прошедших semantic checks
data_quality_volume_anomaly{dataset="bookings"}      0      # 1 = аномалия объема

# ML-специфичные
ml_feature_drift_score{model="surge_pricing"}        0.05   # < 0.1 = ОК
ml_prediction_drift_score{model="fraud_detection"}   0.03   # < 0.1 = ОК
ml_feature_freshness_seconds{feature="zone_active_drivers"} 3
```

Алерты DQ:

| Алерт                            | Severity | Порог                        | Действие                                |
| -------------------------------- | -------- | ---------------------------- | --------------------------------------- |
| DQ score < 0.9                   | P1       | Критическое падение качества | Блокировка ML retraining, расследование |
| DQ score < 0.95                  | P2       | Деградация качества          | Alert -> Data Engineer                  |
| Feature drift > 0.2              | P2       | Сильный drift                | Ускоренный retrain модели               |
| Feature freshness > 60s (online) | P2       | Устаревшие фичи              | Проверка Flink job                      |
| Volume anomaly = 1               | P2       | Аномальный объем данных      | Проверка producer-а                     |

## Альтернативы

1. Lambda Architecture (полная) - отдельные stream и batch слои с раздельной логикой. Обеспечивает максимальную гарантию, но удваивает стоимость разработки и поддержки. Kappa+ позволяет использовать Spark только там, где stream неэффективен.
2. Databricks Lakehouse - managed platform с Unity Catalog, Delta Lake, MLflow. Снижает операционные затраты, но создает vendor lock-in и дорого при масштабе GoFuture (~150K events/s).
3. Self-hosted MLflow + custom Feature Store - полный контроль, но Feast уже решает проблему online/offline store с Kubernetes-native deployment.
4. dbt вместо Spark для трансформаций - проще для SQL-трансформаций, но не подходит для ML training pipelines и feature engineering на Python/Scala.
5. Monte Carlo / Soda вместо Great Expectations - SaaS-решения с better UI, но дороже и требуют отправки метаданных во внешний сервис (compliance concern).

## Компромиссы

- Iceberg + S3 добавляют задержку (flush interval ~30 сек) по сравнению с прямой записью в ClickHouse. Для BI это приемлемо; real-time дашборды питаются из ClickHouse (stream path).
- Feast Online Store (Redis) добавляет еще один Redis-кластер для ML-фичей (помимо Redis для surge/geo). Митигация: отдельный namespace в том же Redis-кластере.
- Great Expectations замедляет batch pipeline (~5-10% overhead). Митигация: DQ checks выполняются параллельно с основным pipeline; блокируют только downstream (не основной поток).
- Data Contracts требуют координации между командами (producer обязан поддерживать контракт). Митигация: contract-as-code в CI/CD, автоматическая проверка при деплое.
- OpenLineage создает дополнительный объем метаданных. Митигация: retention lineage events - 90 дней, sampling для высокочастотных pipeline-ов.
- Sampling driver.location (каждое 10-е событие в Data Lake) теряет 90% данных, но при 100K events/s полная запись требует ~8 TB/день. Для ML достаточно 10% sample; для real-time используется Flink (full stream).
- Shadow Deployment ML-моделей удваивает inference-нагрузку на 5% трафика. Митигация: shadow inference асинхронно (не на критическом пути), результаты записываются в лог для сравнения.
