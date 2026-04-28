# ADR-012: Мультитенантная платформа для партнеров

## Контекст

GoFuture работает по партнерской модели: корпоративные клиенты (партнеры) запускают сервисы заказа поездок под собственным брендом, используя платформу GoFuture в качестве бэкенда. Примеры партнеров:

- Авиакомпания - заказ трансфера из аэропорта через приложение авиакомпании.
- Банк - кешбэк-программа при оплате поездок картой банка.
- Отель - вызов такси через ресепшн-приложение сети отелей.
- Корпоративный клиент - бизнес-аккаунт с лимитами, согласованиями и отчетностью.

Каждый партнер требует:

- Полной изоляции данных - партнер A не должен видеть пассажиров, поездки и платежи партнера B.
- Кастомизации - собственный брендинг, тарифы, бизнес-правила, SLA.
- SSO - сотрудники партнера входят через корпоративный Identity Provider.
- Быстрого подключения - от подписания договора до первой поездки < 5 рабочих дней.

Текущая система (ADR-002) включает Identity Service с поддержкой мультитенантности, но без детализации модели изоляции, ролевого доступа и процесса онбординга.

## Решение

### 1. Модель изоляции данных между партнерами

Диаграмма: [diagrams/tenant-data-isolation.puml](diagrams/tenant-data-isolation.puml)

#### Выбор модели: Hybrid Isolation (логическая + физическая)

| Слой                         | Модель изоляции                                                                        | Обоснование                                                                             |
| ---------------------------- | -------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| Compute (K8s pods)           | Shared - общие pods для всех тенантов                                                  | Экономия ресурсов; tenant_id передается в JWT и propagated через все запросы            |
| Application                  | Tenant-aware middleware - каждый запрос привязан к tenant_id                           | Единая кодовая база; tenant context извлекается из JWT в первом middleware              |
| Операционные БД (PostgreSQL) | Schema-per-tenant (для крупных) / Shared schema + Row-Level Security (для стандартных) | Баланс между изоляцией и операционными затратами                                        |
| Kafka                        | Shared topics - tenant_id в ключе/заголовке                                            | Отдельные топики per tenant непрактичны при 100+ партнерах; фильтрация consumer-ом      |
| Кеши (Redis)                 | Shared cluster - namespace per tenant (prefix `{tenant_id}:`)                          | Экономия ресурсов; TTL per tenant                                                       |
| Data Lake (S3)               | Partition per tenant (`s3://…/tenant_id=xxx/`)                                         | Физическая изоляция файлов; IAM policies per prefix                                     |
| Feature Store / ML           | Shared models - tenant_id как фича                                                     | Отдельные модели per tenant неэффективны; tenant-specific поведение кодируется как фича |

#### Tier-система для партнеров

| Tier       | Изоляция БД                   | Compute                                     | Кастомизация                                          | SLA    | Примеры                     |
| ---------- | ----------------------------- | ------------------------------------------- | ----------------------------------------------------- | ------ | --------------------------- |
| Enterprise | Dedicated schema (PostgreSQL) | Dedicated namespace (K8s) с resource quotas | Полная (тарифы, бизнес-правила, брендинг, custom API) | 99,99% | Авиакомпании, крупные банки |
| Business   | Shared schema + RLS           | Shared pods с tenant resource limits        | Средняя (тарифы, брендинг, лимиты)                    | 99,95% | Отели, средний бизнес       |
| Standard   | Shared schema + RLS           | Shared pods                                 | Базовая (брендинг, лимиты)                            | 99,9%  | Малый бизнес, стартапы      |

#### Row-Level Security (RLS) в PostgreSQL

```sql
-- Включение RLS на таблице bookings
ALTER TABLE bookings ENABLE ROW LEVEL SECURITY;

-- Политика: пользователь видит только записи своего тенанта
CREATE POLICY tenant_isolation ON bookings
    USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

-- При каждом запросе middleware устанавливает tenant context
SET app.current_tenant_id = 'partner-uuid-123';
```

Для Enterprise-тенантов с dedicated schema:

```sql
-- Каждый Enterprise-партнер имеет свою схему
CREATE SCHEMA tenant_airline_xyz;

-- Таблицы создаются в схеме партнера
CREATE TABLE tenant_airline_xyz.bookings (LIKE public.bookings INCLUDING ALL);

-- search_path устанавливается middleware
SET search_path TO tenant_airline_xyz, public;
```

#### Изоляция на уровне Kafka

Все события содержат `tenant_id` в заголовке и Avro-payload:

```json
{
  "headers": {
    "tenant_id": "partner-uuid-123",
    "trace_id": "...",
    "event_type": "BookingCreated"
  },
  "payload": {
    "tenant_id": "partner-uuid-123",
    "booking_id": "...",
    "..."
  }
}
```

Consumer-ы фильтруют события по tenant_id. Enterprise-тенанты могут получать отдельный consumer group с гарантированным throughput.

### 2. IAM-система с ролевым доступом и SSO

C2-диаграмма с IAM: [C2-multitenant-iam.puml](C2-multitenant-iam.puml)

#### 2.1 Архитектура Identity & Access Management

IAM построена на базе Identity Service (ADR-002) с расширением для мультитенантности:

| Компонент               | Технология               | Назначение                                                                                     |
| ----------------------- | ------------------------ | ---------------------------------------------------------------------------------------------- |
| Identity Provider (IdP) | Keycloak                 | Управление пользователями, ролями, группами; выдача JWT-токенов; Federation с партнерскими IdP |
| SSO Gateway             | Keycloak Broker          | SAML 2.0 / OIDC federation с корпоративными IdP партнеров (Azure AD, Okta, Google Workspace)   |
| Policy Engine           | Open Policy Agent (OPA)  | Declarative policies (Rego): RBAC + ABAC; per-tenant permissions; data access rules            |
| API Gateway             | Kong / Envoy             | JWT-валидация, tenant context extraction, rate limiting per tenant                             |
| Audit Log               | Immutable append-only DB | Все auth-события, изменения ролей, доступ к PII                                                |

#### 2.2 Модель аутентификации

Поток аутентификации (SSO):

```
1. Сотрудник партнера -> Партнерское приложение -> "Войти через SSO"
2. Redirect -> Keycloak (GoFuture IdP)
3. Keycloak -> Redirect -> Корпоративный IdP партнера (Azure AD / Okta)
4. Сотрудник вводит корп. логин/пароль -> IdP партнера верифицирует
5. IdP партнера -> SAML Assertion / OIDC Token -> Keycloak
6. Keycloak маппит external claims -> GoFuture roles (по настройкам тенанта)
7. Keycloak выдает JWT:
   {
     "sub": "user-uuid",
     "tenant_id": "partner-uuid-123",
     "roles": ["partner_admin", "booking_manager"],
     "realm": "partner-airline-xyz",
     "sso_provider": "azure-ad",
     "exp": 1713400000
   }
8. JWT -> API Gateway -> tenant context propagation -> все сервисы
```

Для пассажиров и водителей (не SSO):

- Регистрация через телефон + OTP (SMS).
- JWT содержит `tenant_id` партнера, через которого пользователь пришел.
- Пользователь может быть привязан к нескольким тенантам (например, пассажир использует и приложение авиакомпании, и банковское приложение).

#### 2.3 Ролевая модель (RBAC)

Иерархия ролей:

```
GoFuture Platform
   platform_super_admin          # GoFuture: полный доступ
   platform_admin                # GoFuture: управление тенантами
   platform_support              # GoFuture: поддержка пользователей

   Tenant (Partner)
       partner_owner             # Владелец аккаунта партнера
       partner_admin             # Администратор партнера
       partner_finance           # Финансовый менеджер
       partner_ops               # Операционный менеджер
       partner_support           # Поддержка партнера
       partner_analyst           # Аналитик (read-only)
       partner_developer         # Разработчик (API-интеграция)

       End Users
          passenger             # Пассажир
          driver                # Водитель
```

#### 2.4 Таблица ролей и уровней доступа

| Роль                 | Bookings          | Drivers           | Payments         | Pricing   | Users (tenant)    | Analytics    | Settings  | API Keys  | Audit Log   |
| -------------------- | ----------------- | ----------------- | ---------------- | --------- | ----------------- | ------------ | --------- | --------- | ----------- |
| platform_super_admin | RW (все тенанты)  | RW (все)          | RW (все)         | RW (все)  | RW (все)          | R (все)      | RW (все)  | RW (все)  | R (все)     |
| platform_admin       | R (все тенанты)   | R (все)           | R (все)          | R (все)   | RW (все)          | R (все)      | RW (все)  | -         | R (все)     |
| platform_support     | R (все тенанты)   | R (все)           | R (все)          | -         | R (все)           | -            | -         | -         | R (все)     |
| partner_owner        | RW (свой тенант)  | RW (свой)         | RW (свой)        | RW (свой) | RW (свой)         | R (свой)     | RW (свой) | RW (свой) | R (свой)    |
| partner_admin        | RW (свой тенант)  | RW (свой)         | R (свой)         | RW (свой) | RW (свой)         | R (свой)     | RW (свой) | R (свой)  | R (свой)    |
| partner_finance      | R (свой тенант)   | -                 | RW (свой)        | R (свой)  | -                 | R (финансы)  | -         | -         | R (финансы) |
| partner_ops          | RW (свой тенант)  | R (свой)          | R (свой)         | R (свой)  | R (свой)          | R (операции) | R (свой)  | -         | -           |
| partner_support      | R (свой тенант)   | R (свой)          | R (свой)         | -         | R (свой)          | -            | -         | -         | -           |
| partner_analyst      | R (свой тенант)   | R (свой)          | R (агрег.)       | R (свой)  | -                 | R (свой)     | -         | -         | -           |
| partner_developer    | -                 | -                 | -                | -         | -                 | -            | R (свой)  | RW (свой) | -           |
| passenger            | RW (свои поездки) | -                 | R (свои платежи) | -         | RW (свой профиль) | -            | -         | -         | -           |
| driver               | RW (свои поездки) | RW (свой профиль) | R (свои выплаты) | -         | RW (свой профиль) | -            | -         | -         | -           |

Обозначения: R - чтение, RW - чтение + запись, - - нет доступа, "свой" - только данные своего тенанта, "агрег." - только агрегированные данные (без PII).

#### 2.5 Policy Engine (Open Policy Agent)

OPA обеспечивает fine-grained access control поверх RBAC:

```rego
# policy/tenant_access.rego
package gofuture.authz

# Правило: пользователь видит только данные своего тенанта
allow {
    input.method == "GET"
    input.path = ["api", "v1", "bookings", _]
    token := input.token
    token.tenant_id == input.resource.tenant_id
}

# Правило: partner_finance не может создавать bookings
deny {
    input.method == "POST"
    input.path = ["api", "v1", "bookings"]
    token := input.token
    token.roles[_] == "partner_finance"
}

# Правило: platform_support может читать любой тенант, но не писать
allow {
    input.method == "GET"
    token := input.token
    token.roles[_] == "platform_support"
}

# Правило: partner_analyst видит только агрегированные платежи
allow {
    input.method == "GET"
    input.path = ["api", "v1", "payments", "summary"]
    token := input.token
    token.roles[_] == "partner_analyst"
    token.tenant_id == input.resource.tenant_id
}
```

API Gateway запрашивает OPA при каждом запросе (sidecar, latency < 2 мс).

### 3. Автоматизированный процесс онбординга (Tenant Provisioning)

Диаграмма: [diagrams/onboarding-flow.puml](diagrams/onboarding-flow.puml)

#### 3.1 Шаги онбординга

| Шаг | Действие                                                        | Ответственный                | Автоматизация                                     | Время    |
| --- | --------------------------------------------------------------- | ---------------------------- | ------------------------------------------------- | -------- |
| 1   | Регистрация партнера через Partner Portal                       | Партнер                      | Полная                                            | 5 мин    |
| 2   | Верификация (KYC/KYB)                                           | Compliance team              | Полуавтоматическая (API проверки + ручной review) | 1-2 дня  |
| 3   | Выбор tier (Standard/Business/Enterprise) и подписание договора | Sales + Партнер              | DocuSign API                                      | 1 день   |
| 4   | Provisioning тенанта                                            | Tenant Provisioning Service  | Полная автоматизация                              | < 10 мин |
| 5   | Настройка SSO (если нужно)                                      | Партнер + Platform team      | Self-service portal + автоматическая верификация  | 30 мин   |
| 6   | Кастомизация (тарифы, брендинг, правила)                        | Партнер через Partner Portal | Self-service                                      | 1-2 часа |
| 7   | Sandbox-тестирование                                            | Партнер                      | Автоматический sandbox                            | 1-2 дня  |
| 8   | Go-live (активация production)                                  | Партнер + GoFuture ops       | Один клик + автопроверки                          | 5 мин    |

Итого: < 5 рабочих дней (большая часть - ожидание KYC и тестирование партнером).

#### 3.2 Tenant Provisioning Service (автоматизация шага 4)

При создании нового тенанта сервис выполняет:

```
Provisioning Pipeline (автоматический, < 10 мин):

1. CREATE TENANT RECORD
   Identity Service: создать tenant entity, realm в Keycloak

2. PROVISION DATABASE
   Standard/Business: CREATE RLS policy для нового tenant_id
   Enterprise: CREATE SCHEMA tenant_{name}; CREATE TABLES...

3. PROVISION IAM
   Keycloak: создать realm/client для партнера
   Создать роли (partner_owner, partner_admin, ...)
   Создать первого пользователя (partner_owner) -> отправить invite
   Настроить SSO broker (если указан IdP партнера)

4. PROVISION CONFIGURATION
   Pricing Service: создать тарифный план (default или custom)
   Notification Service: создать шаблоны с брендингом
   Fraud Service: применить default fraud-правила
   API Gateway: создать rate limiting policy per tenant

5. PROVISION MONITORING
   Grafana: создать tenant-specific dashboard
   Prometheus: настроить алерты с label tenant_id
   Alertmanager: настроить routing для алертов партнера

6. PROVISION SANDBOX
   Создать sandbox environment (отдельный namespace)
   Сгенерировать тестовые API keys
   Загрузить seed data (тестовые водители, геозоны)

7. SEND WELCOME
   Email: welcome + ссылка на Partner Portal
   API docs: endpoint + sandbox credentials
   Slack/Teams: пригласить в канал поддержки
```

Provisioning реализован как Saga (аналогично Booking Saga из ADR-007): каждый шаг идемпотентен, при ошибке - компенсация (удаление созданных ресурсов).

#### 3.3 Partner Portal (Self-Service)

| Раздел     | Функциональность                                         | Доступ                                    |
| ---------- | -------------------------------------------------------- | ----------------------------------------- |
| Dashboard  | KPI партнера: поездки, выручка, NPS, SLA compliance      | partner_owner, partner_admin, partner_ops |
| Branding   | Логотип, цвета, название, шаблоны уведомлений            | partner_owner, partner_admin              |
| Pricing    | Тарифы, промокоды, скидки (в рамках разрешенных лимитов) | partner_owner, partner_admin              |
| Users      | Управление сотрудниками, ролями, SSO-настройки           | partner_owner, partner_admin              |
| API        | API keys, webhooks, документация, sandbox                | partner_developer                         |
| Finance    | Счета, акты, выписки, настройки биллинга                 | partner_owner, partner_finance            |
| Support    | Тикеты, FAQ, статус платформы                            | Все роли партнера                         |
| Compliance | Согласие на обработку данных, DPA, data export           | partner_owner                             |

### 4. Мониторинг мультитенантного окружения

#### 4.1 Tenant-aware метрики

Все метрики дополняются label `tenant_id` для изоляции мониторинга:

```
# Метрика с tenant label
http_requests_total{service="booking", tenant_id="partner-123", status="200"} 15420
http_request_duration_seconds{service="booking", tenant_id="partner-123", quantile="0.95"} 0.180
booking_created_total{tenant_id="partner-123"} 3200
payment_success_rate{tenant_id="partner-123"} 0.987
```

#### 4.2 Метрики per tenant

| Категория          | Метрики                                                              | Алерт                        |
| ------------------ | -------------------------------------------------------------------- | ---------------------------- |
| Использование      | requests/sec, bookings/day, active users, active drivers             | Volume > 2x plan limit (P3)  |
| Производительность | API latency p95, booking confirmation time, payment time             | p95 > SLA threshold (P2)     |
| Ошибки             | Error rate per tenant, 4xx rate, 5xx rate                            | Error rate > 1% (P2)         |
| Бизнес             | Booking success rate, cancellation rate, driver match rate           | Conversion < 80% (P3)        |
| Квоты              | API rate limit usage %, storage usage, events/sec                    | > 80% quota (P3), > 95% (P2) |
| Финансы            | Revenue per tenant, outstanding balance, payout status               | Outstanding > 30 days (P3)   |
| Безопасность       | Failed auth attempts, suspicious API patterns, data access anomalies | > 10 failed auth/min (P2)    |

#### 4.3 Дашборды мониторинга

| Дашборд                      | Аудитория                     | Содержимое                                                                         |
| ---------------------------- | ----------------------------- | ---------------------------------------------------------------------------------- |
| Tenant Overview (per tenant) | Партнер (Partner Portal)      | Поездки, выручка, SLA compliance, ошибки - только данные партнера                  |
| All Tenants Overview         | GoFuture ops (platform_admin) | Сводка по всем тенантам: usage, health, noisy neighbors                            |
| Noisy Neighbor Detection     | GoFuture SRE                  | Тенанты, потребляющие > fair share ресурсов: CPU, DB connections, Kafka throughput |
| Tenant Onboarding            | GoFuture ops                  | Статус provisioning, pipeline health, time-to-activate                             |
| Tenant Billing               | GoFuture finance              | Revenue per tenant, usage vs plan, overage                                         |
| Tenant Security              | GoFuture security             | Auth anomalies, data access patterns, compliance status                            |

#### 4.4 Noisy Neighbor Detection

Мультитенантная среда подвержена проблеме "шумного соседа" - один тенант потребляет непропорционально много ресурсов:

| Ресурс           | Fair share                           | Обнаружение                   | Митигация                              |
| ---------------- | ------------------------------------ | ----------------------------- | -------------------------------------- |
| API RPS          | Per-tenant rate limit (в плане)      | Kong rate limiter             | 429 Too Many Requests -> alert partner |
| DB connections   | PgBouncer per-tenant pool            | Connection count per tenant   | Pool exhaustion -> queue; alert        |
| DB query time    | Statement timeout per tenant         | Slow query log + tenant label | Kill query > timeout; alert            |
| Kafka throughput | Quota per tenant (producer/consumer) | Kafka quotas                  | Throttling; alert                      |
| CPU/Memory       | K8s resource quotas (Enterprise)     | cgroup metrics per namespace  | OOMKill -> reschedule; alert           |
| Storage          | Quota per tenant (S3 + DB)           | Periodic size check           | Block writes > quota; alert            |

### 5. Tenant Lifecycle Management

| Фаза           | Состояние        | Описание                                                          |
| -------------- | ---------------- | ----------------------------------------------------------------- |
| Provisioning   | `PROVISIONING`   | Ресурсы создаются                                                 |
| Sandbox        | `SANDBOX`        | Тестовый режим, данные не реальные                                |
| Active         | `ACTIVE`         | Production, обрабатывает реальные поездки                         |
| Suspended      | `SUSPENDED`      | Временно приостановлен (неоплата, нарушение) - API возвращает 403 |
| Deprovisioning | `DEPROVISIONING` | Удаление данных (GDPR/LGPD), отключение ресурсов                  |
| Archived       | `ARCHIVED`       | Данные удалены; audit log сохранен на 5 лет                       |

Деактивация тенанта (обратная Saga):

1. Уведомление партнера (30 дней предупреждение).
2. Экспорт данных (Data Portability - GDPR Art. 20).
3. Деактивация API keys, SSO.
4. Soft-delete данных (30 дней grace period).
5. Hard-delete данных + purge кешей.
6. Удаление DB schema / RLS policies.
7. Удаление Grafana дашбордов, алертов.
8. Архивация audit log.

## Альтернативы

1. Полная физическая изоляция (отдельный K8s namespace + DB per tenant) - максимальная изоляция, но дорого: при 100+ тенантах создает 100+ PostgreSQL-инстансов, сотни pods. Оправдано только для Enterprise-tier.
2. Только shared schema + RLS (без schema-per-tenant) - проще операционно, но крупные партнеры требуют гарантий изоляции; RLS - soft boundary, ошибка в middleware может привести к data leak.
3. Auth0 / Okta вместо Keycloak - SaaS, меньше операционных затрат, но дороже при масштабе (per-user pricing), данные хранятся у вендора (compliance concern).
4. Casbin вместо OPA - проще для чистого RBAC, но OPA гибче для ABAC-сценариев (tenant-aware policies, time-based access).
5. Ручной onboarding - проще реализовать, но не масштабируется при плане 50+ партнеров в год; time-to-activate > 2 недель вместо 5 дней.

## Компромиссы

- Hybrid isolation усложняет архитектуру (два пути: RLS и schema-per-tenant). Митигация: Tenant Provisioning Service абстрагирует разницу; для сервисов изоляция прозрачна (middleware устанавливает context).
- tenant_id в каждом запросе увеличивает payload JWT на ~40 байт. Пренебрежимо при среднем размере JWT ~1 KB.
- OPA sidecar добавляет ~1-2 мс к каждому запросу. Митигация: кеширование policy decisions (5 сек TTL); при недоступности OPA - fallback на RBAC из JWT.
- Shared Kafka topics не гарантируют физическую изоляцию событий. Партнер потенциально может получить чужое событие при ошибке consumer-а. Митигация: tenant_id проверяется в каждом consumer; Enterprise-тенанты могут получить dedicated consumer group.
- Keycloak требует выделенного кластера (HA mode, 3+ instances). Это операционные затраты, но дает полный контроль над IdP и соответствие compliance.
- Noisy neighbor detection требует per-tenant метрик, что увеличивает cardinality Prometheus. Митигация: агрегация по tier, детализация только для top-20 тенантов по usage.
