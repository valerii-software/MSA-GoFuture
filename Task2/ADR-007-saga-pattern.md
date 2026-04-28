# ADR-007: Saga-паттерн для критических бизнес-процессов

## Контекст

В микросервисной архитектуре "GoFuture" бизнес-процессы охватывают несколько сервисов: создание бронирования затрагивает Booking, Fraud, Pricing, Geography, Driver; оплата поездки - Payments, Fraud, Booking. Распределенные транзакции (2PC) неприменимы из-за снижения доступности и throughput. Необходим паттерн Saga для обеспечения согласованности данных между сервисами.

## Требования

Выбрать вид Saga, проработать последовательности событий и компенсирующие действия для критических бизнес-процессов платформы.

## Решение

### Выбор вида Saga: Orchestration

Выбрана Saga с оркестрацией (Orchestration-based Saga).

| Критерий                | Orchestration                                        | Choreography                                            |
| ----------------------- | ---------------------------------------------------- | ------------------------------------------------------- |
| Явная видимость потока  | Да - логика сосредоточена в оркестраторе             | Нет - распределена по сервисам                          |
| Простота мониторинга    | Высокая - один компонент отслеживает всю транзакцию  | Низкая - нужно коррелировать события из разных сервисов |
| Управление компенсацией | Централизованное - оркестратор знает, что откатывать | Распределенное - каждый сервис сам решает               |
| Сложность при > 4 шагах | Линейный рост                                        | Экспоненциальный рост связей                            |
| Связанность             | Оркестратор знает о всех участниках                  | Участники независимы                                    |

Обоснование: Бизнес-процессы GoFuture содержат 5-7 шагов с условной логикой (если фрод - отменить, если нет водителя - ждать/отменить). Хореография при таком количестве участников создает "спагетти событий", которое крайне сложно отлаживать и мониторить. Для платформы с 99,95% SLA необходима полная видимость состояния каждой транзакции.

Оркестратор реализуется как stateful-сервис с хранением состояния саги в своей БД. Он взаимодействует с участниками через Kafka (command -> reply).

### Saga 1: Booking Saga (создание бронирования)

Триггер: Пассажир нажал "Заказать поездку"  
Оркестратор: Booking Service  
Участники: Fraud, Pricing, Geography, Driver, Notification  
Таймаут саги: 60 сек (после - автоматическая отмена)

#### Диаграмма последовательности

Диаграмма: [diagrams/booking-saga-sequence.puml](diagrams/booking-saga-sequence.puml)

#### Шаги саги

| Шаг | Команда (Command)      | Сервис       | Успех (Reply)      | Компенсация                              | Описание                                       |
| --- | ---------------------- | ------------ | ------------------ | ---------------------------------------- | ---------------------------------------------- |
| 1   | `CreateBookingCmd`     | Booking      | `BookingCreated`   | -                                        | Создание записи в статусе PENDING              |
| 2   | `CheckBookingFraudCmd` | Fraud        | `FraudCheckPassed` | `CancelBookingCmd`                       | Проверка пассажира и параметров заказа на фрод |
| 3   | `CalculatePriceCmd`    | Pricing      | `PriceCalculated`  | `CancelBookingCmd`                       | Расчет стоимости с учетом surge                |
| 4   | `CalculateRouteCmd`    | Geography    | `RouteCalculated`  | `CancelBookingCmd`                       | Построение маршрута и расчет ETA               |
| 5   | `FindDriverCmd`        | Driver       | `DriverAssigned`   | `ReleaseDriverCmd` -> `CancelBookingCmd` | "Умный" подбор водителя                        |
| 6   | `NotifyPartiesCmd`     | Notification | `NotificationSent` | - (best effort)                          | Уведомление пассажира и водителя               |
| 7   | -                      | Booking      | `BookingConfirmed` | -                                        | Перевод в статус CONFIRMED                     |

#### События саги (через Kafka)

Топик команд: `saga.booking.commands.v1`  
Топик ответов: `saga.booking.replies.v1`

```json
// Команда: CheckBookingFraudCmd
{
  "event_id": "uuid",
  "event_type": "CheckBookingFraudCmd",
  "saga_id": "saga-uuid",
  "step": 2,
  "correlation_id": "trace-id",
  "payload": {
    "booking_id": "booking-uuid",
    "passenger_id": "user-uuid",
    "pickup_location": {"lat": 55.75, "lng": 37.62},
    "dropoff_location": {"lat": 55.73, "lng": 37.59},
    "estimated_price": 450.00
  }
}

// Ответ: FraudCheckPassed
{
  "event_id": "uuid",
  "event_type": "FraudCheckPassed",
  "saga_id": "saga-uuid",
  "step": 2,
  "correlation_id": "trace-id",
  "payload": {
    "booking_id": "booking-uuid",
    "risk_score": 0.05,
    "decision": "APPROVE"
  }
}
```

#### Сценарии компенсации

Диаграмма: [diagrams/booking-saga-compensation.puml](diagrams/booking-saga-compensation.puml)

Сценарий A: Фрод обнаружен (шаг 2) - CancelBookingCmd (статус: CANCELLED_FRAUD) + NotifyPassengerCmd.

Сценарий B: Нет доступных водителей (шаг 5, таймаут) - RetryFindDriverCmd до 3 раз с расширением радиуса. Если все попытки неуспешны: CancelBookingCmd (статус: CANCELLED_NO_DRIVER) + NotifyPassengerCmd.

Сценарий C: Водитель отменил после назначения - ReleaseDriverCmd (штраф к рейтингу) + RetryFindDriverCmd (новый поиск) + NotifyPassengerCmd.

#### Машина состояний Booking Saga

Диаграмма: [diagrams/booking-saga-statemachine.puml](diagrams/booking-saga-statemachine.puml)

### Saga 2: Payment Saga (оплата поездки)

Триггер: Событие `TripCompleted`  
Оркестратор: Payments Service  
Участники: Fraud, Яндекс Пэй (через Payments), Driver (earnings), Notification  
Таймаут саги: 120 сек

#### Шаги саги

| Шаг | Команда                  | Сервис                   | Успех                | Компенсация             | Описание                                    |
| --- | ------------------------ | ------------------------ | -------------------- | ----------------------- | ------------------------------------------- |
| 1   | `InitiatePaymentCmd`     | Payments                 | `PaymentInitiated`   | -                       | Создание платежа в статусе PENDING          |
| 2   | `CheckPaymentFraudCmd`   | Fraud                    | `FraudCheckPassed`   | `CancelPaymentCmd`      | Проверка транзакции на фрод                 |
| 3   | `ChargePassengerCmd`     | Payments (-> Яндекс Пэй) | `PaymentCompleted`   | `RefundPassengerCmd`    | Списание с карты пассажира                  |
| 4   | `CalculateEarningsCmd`   | Payments                 | `EarningsCalculated` | `ReverseEarningsCmd`    | Расчет заработка водителя (цена - комиссия) |
| 5   | `CreditDriverBalanceCmd` | Payments                 | `BalanceCredited`    | `DebitDriverBalanceCmd` | Зачисление на баланс водителя               |
| 6   | `NotifyPaymentCmd`       | Notification             | `NotificationSent`   | - (best effort)         | Уведомление пассажира и водителя            |

#### Сценарии компенсации

Диаграмма: [diagrams/payment-saga-sequence.puml](diagrams/payment-saga-sequence.puml)

Сценарий A: Фрод при оплате - CancelPaymentCmd (статус: CANCELLED_FRAUD) + HoldBookingCmd (расследование) + NotifyCmd ("Платеж заблокирован").

Сценарий B: Ошибка платежного шлюза - RetryChargeCmd до 3 раз (5, 15, 30 сек). Если все попытки неуспешны: MarkPaymentPendingCmd + NotifyCmd + Celery retry через 1 час.

Сценарий C: Ошибка зачисления на баланс водителя - НЕ делаем refund пассажиру. RetryCredit до 5 раз. Если не удалось - создает инцидент + NotifyCmd -> Support.

### Saga 3: Payout Saga (выплата водителю)

Триггер: По расписанию (ежедневно/еженедельно) или по запросу водителя  
Оркестратор: Payments Service  
Участники: API Банка (через Payments), Notification  
Таймаут саги: 300 сек (банковские операции медленнее)

| Шаг | Команда             | Сервис                  | Успех                   | Компенсация         | Описание                                      |
| --- | ------------------- | ----------------------- | ----------------------- | ------------------- | --------------------------------------------- |
| 1   | `InitiatePayoutCmd` | Payments                | `PayoutInitiated`       | -                   | Создание выплаты, блокировка суммы на балансе |
| 2   | `TransferToBankCmd` | Payments (-> API Банка) | `BankTransferCompleted` | `UnblockBalanceCmd` | Перевод на счет водителя                      |
| 3   | `ConfirmPayoutCmd`  | Payments                | `PayoutCompleted`       | -                   | Списание с баланса, запись в аудит            |
| 4   | `NotifyPayoutCmd`   | Notification            | `NotificationSent`      | -                   | Уведомление водителя                          |

### Хранение состояния Saga

Оркестратор хранит состояние каждой саги в своей БД:

```sql
CREATE TABLE saga_instances (
    saga_id         UUID PRIMARY KEY,
    saga_type       VARCHAR(50) NOT NULL,       -- 'BOOKING', 'PAYMENT', 'PAYOUT'
    current_step    INT NOT NULL,
    state           VARCHAR(30) NOT NULL,        -- 'RUNNING', 'COMPENSATING', 'COMPLETED', 'FAILED'
    payload         JSONB NOT NULL,              -- Данные саги (booking_id, payment_id и т.д.)
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    timeout_at      TIMESTAMP NOT NULL,          -- Дедлайн саги
    retry_count     INT DEFAULT 0,
    last_error      TEXT
);

CREATE TABLE saga_step_log (
    id              BIGSERIAL PRIMARY KEY,
    saga_id         UUID REFERENCES saga_instances(saga_id),
    step            INT NOT NULL,
    command_type    VARCHAR(100) NOT NULL,
    reply_type      VARCHAR(100),
    status          VARCHAR(30) NOT NULL,        -- 'SENT', 'SUCCESS', 'FAILED', 'COMPENSATED'
    sent_at         TIMESTAMP NOT NULL,
    replied_at      TIMESTAMP,
    payload         JSONB
);
```

Timeout handler: Celery Beat задача каждые 10 сек проверяет `saga_instances` WHERE `state = 'RUNNING' AND timeout_at < NOW()` и инициирует компенсацию для просроченных саг.

## Альтернативы

1. Choreography Saga - участники реагируют на события друг друга без оркестратора. Снижает связанность, но при 5-7 шагах создает сложную сеть событий, затрудняющую отладку и мониторинг. Не подходит для процессов с условной логикой (retry, timeout, расширение радиуса поиска).
2. Распределенные транзакции (2PC / XA) - гарантируют ACID, но блокируют ресурсы на время транзакции, снижают throughput и доступность. Неприменимы при 500K конкурентных поездок.
3. Гибридный подход (оркестрация для Booking/Payment, хореография для Notification) - возможен, но усложняет ментальную модель. На данном этапе единообразие важнее оптимизации.

## Компромиссы

- Оркестратор - single point of knowledge - знает о всех участниках. Митигация: оркестратор stateful, горизонтально масштабируемый через партиционирование saga_id.
- Eventual consistency - между шагами саги данные не согласованы. Например, после DriverAssigned, но до BookingConfirmed, возможна гонка. Митигация: идемпотентные шаги, таймауты, retry.
- Latency - последовательное выполнение шагов через Kafka добавляет ~10-50 мс на каждый hop. Booking Saga (6 шагов) -> ~100-300 мс. Для UX приемлемо (пользователь видит "Ищем водителя"), но шаги 2-4 можно распараллелить (Fraud + Price + Route одновременно), сократив latency до ~150 мс.
- Saga timeout 60 сек - большую часть занимает поиск водителя (до 30 сек \* 3 попытки). Пассажир видит индикатор прогресса.

### Оптимизация: параллельные шаги

Шаги 2, 3, 4 (Fraud, Price, Route) не зависят друг от друга и могут выполняться параллельно:

Диаграмма параллельных шагов: [diagrams/booking-saga-sequence.puml](diagrams/booking-saga-sequence.puml)

Это сокращает latency Booking Saga с ~300 мс до ~150 мс (шаги 2+3+4 выполняются за max(2,3,4) вместо sum(2,3,4)).
