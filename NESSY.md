# DBQ Project Context

## Project Overview

**DBQ** (Database-backed Queue) — это многопоточная очередь сообщений на основе PostgreSQL для транзакционной публикации и обработки сообщений.

**Основная идея:** хранить сообщения в БД, позволять независимым подпискам обрабатывать их в собственных транзакциях, и поддерживать минимальный API для встраивания в приложение без необходимости развёртывания отдельного брокера.

### Ключевые возможности

- Транзакционная публикация сообщений в ту же БД, что и бизнес-данные
- Несколько независимых подписок на одну очередь
- Политики retry, blocking и non-retryable ошибок
- Генерация PostgreSQL DDL для очередей и подписок
- Управление партициями PostgreSQL (создание/удаление)
- OpenTelemetry интеграция для трассировки

### Модули

| Модуль | Описание |
|--------|----------|
| `core` | Domain model, DBQ API, retry/blocking политики, consumer runtime, unit-тесты |
| `pg` | PostgreSQL `QueryService`, JSONB конвертация, DDL генератор, partition manager |
| `spring` | Spring JDBC persistence adapter и Spring transaction adapter |
| `pg-cli` | Standalone CLI для генерации PostgreSQL DDL |
| `otel` | OpenTelemetry propagation и consumer tracing интеграция |
| `integration-tests` | End-to-end тесты с Testcontainers и PostgreSQL |

### Стек технологий

- **Java 25**
- **Maven** (multi-module project)
- **Spring Framework 6.2.x**
- **Lombok** (annotation processing)
- **Jackson** (JSON serialization)
- **PostgreSQL 42.7.8**
- **Testcontainers** (integration testing)
- **JUnit Jupiter + AssertJ** (testing)
- **JaCoCo** (code coverage)
- **OpenTelemetry 1.54.1**
- **Quartz** (scheduling)

### Структура пакетов

```
org.pak.dbq
├── api/           # Публичный API: QueueManager, Producer, Consumer, Configs
├── spi/           # SPI интерфейсы: QueryService, TransactionService, etc.
└── internal/      # Внутренняя реализация
```

## Building and Running

### Сборка проекта

```bash
# Полная сборка всех модулей
mvn -q clean verify

# Сборка без тестов
mvn -q -pl integration-tests -am -DskipTests package

# Сборка конкретного модуля
mvn -q -pl core -am package
```

### Запуск тестов

```bash
# Unit-тесты core модуля
mvn -q -pl core test

# Unit-тесты otel модуля
mvn -q -pl otel -am test

# Компиляция без запуска integration-тестов (требует Docker)
mvn -q -pl integration-tests -am test -DskipTests

# Полные integration-тесты (требует Docker/Testcontainers)
mvn -q -pl integration-tests -am test
```

### Генерация DDL через CLI

```bash
# Сборка CLI
mvn -q -pl pg-cli -am -DskipTests package

# Генерация SQL для очереди
java -jar pg-cli/target/pg-cli-1.0.0.jar queue --schema public --queue orders

# Генерация SQL для подписки
java -jar pg-cli/target/pg-cli-1.0.0.jar subscription --schema public --queue orders --subscription billing

# Генерация полного SQL (очередь + подписка)
java -jar pg-cli/target/pg-cli-1.0.0.jar all --schema public --queue orders --subscription billing --history-enabled > dbq-orders.sql
```

## Development Conventions

### Coding Style

- **Java 25** с использованием современных возможностей (record patterns, sealed classes)
- **4-space indentation** без tabs
- **Lombok** для boilerplate кода (`@Data`, `@Builder`, `@Value`, `@Slf4j`)
- **UpperCamelCase** для классов, **lowerCamelCase** для методов и полей
- **Value objects** (`QueueName`, `SubscriptionId`, `SchemaName`) используют обёртки вместо raw strings

### Naming Conventions

- Имена очередей: lowercase letters + `-` (например, `orders`, `user-events`)
- Имена подписок: lowercase letters + `-` (например, `billing`, `notification-service`)
- Имена схем БД: lowercase letters + `_` (например, `public`, `message_bus`)
- Партиции: `<table>_yyyy_MM_dd` (UTC дата)

### Testing Practices

#### Парадигма тестирования

- **Unit-тесты** покрывают основную функциональность в `core` модуле
- **Integration-тесты** проверяют поведение с реальной PostgreSQL
- Тесты используют **JUnit Jupiter** и **AssertJ**
- Имена тестов: `*Test` суффикс, описательные имена методов (`publishSubscribeTest`, `testHandleRetryableException`)

#### Покрытие кода

JaCoCo требования (настраиваются в `core/pom.xml`):
- **Lines:** ≥ 75%
- **Branches:** ≥ 60%

#### Приоритетные кейсы для тестирования

Согласно `PleaseWriteTest.MD`, приоритетные области:

1. **`AbstractConsumer.poolLoop`** — обработка исключений, pause logic
2. **`QueueManager` rollback** — откат регистрации при ошибках
3. **`ConsumerStarter.stop()`** — graceful shutdown
4. **`RecordingBatchAcknowledger`** — валидация batch acknowledgment
5. **`Consumer.handleRetryableException()`** — boundary cases для attempt counter

### Commit Guidelines

- Краткие заголовки в повелительном наклонении: `Fix retry delay caching`, `Add drop partition test`
- PR description должен включать:
  - Краткое summary изменений
  - Затронутые модули
  - Команды для верификации
  - Заметки о schema/partition/dependency изменениях

### Architecture Notes

#### Таблицы БД

DBQ использует три типа таблиц:

1. **Queue table** (`queue_<name>`) — партицированная по дате, хранит сообщения
2. **Subscription live table** (`<subscription>`) — не партицирована, хранит активные записи для обработки
3. **Subscription history table** (`<subscription>_history`) — партицирована, хранит обработанные/failed записи (опционально)

#### Consumer Flow

```
poll → lock (FOR UPDATE SKIP LOCKED) → process → complete/retry/fail
```

- `complete` → delete из live или move в history
- `retry` → update attempt + execute_after
- `fail` → delete из live или move в history

#### Политики обработки ошибок

- **`RetryablePolicy`** — возвращает `Optional<Duration>` для задержки retry
- **`NonRetryablePolicy`** — определяет исключения для немедленного fail
- **`BlockingPolicy`** — приостанавливает весь consumer loop

По умолчанию все исключения retryable с exponential backoff (max 1 час).

### Extension Points (SPI)

| Интерфейс | Назначение |
|-----------|------------|
| `QueryService` | Storage implementation (INSERT, SELECT, retry logic) |
| `TableManager` | Partition lifecycle management |
| `PersistenceService` | JDBC abstraction layer |
| `TransactionService` | Transaction boundaries |
| `MessageContextPropagator` | Tracing header injection/extraction |
| `MessageConsumerTelemetry` | Consumer spans и metrics |
| `MessageFactory` | Custom message instantiation |
| `RetryablePolicy` / `NonRetryablePolicy` / `BlockingPolicy` | Exception classification |

### Known Issues & Technical Debt

См. `issues.md` и `PleaseWriteTest.MD` для актуального списка проблем и рекомендаций по тестированию.

### Useful Commands Reference

```bash
# Проверка покрытия тестами
mvn -q -pl core verify

# Поиск по коду
rg "pattern" --type java

# Запуск конкретного теста
mvn -q -pl core test -Dtest=ClassNameTest

# Очистка target директорий
mvn -q clean

# Полная сборка с проверкой качества
mvn -q clean verify

# Сборка с пропускаем тестов (быстрая)
mvn -q -DskipTests package

# Запуск тестов с выводом отчёта JaCoCo
mvn -q -pl core verify

# Поиск упоминаний класса/метода
rg "ClassName" --type java
```

## Integration Testing

### Требования

- **Docker** должен быть запущен
- **Testcontainers** автоматически скачивает образы:
  - `postgres:18-alpine` — основная БД
  - `ghcr.io/shopify/toxiproxy:2.5.0` — симуляция сетевых проблем

### Структура integration-тестов

```
integration-tests/src/test/java/
└── org/pak/dbq/internal/
    ├── BaseIntegrationTest.java    # Базовый класс с настройкой Testcontainers
    ├── ProducerConsumerTest.java   # Тесты публикации/потребления
    ├── PartitionManagementTest.java # Тесты управления партициями
    └── RetryPolicyTest.java        # Тесты retry политик
```

### Особенности

- Каждый тест создаёт изолированную схему/таблицы
- Контейнеры запускаются один раз на весь прогон тестов
- JDBC URL: `jdbc:postgresql://localhost:<dynamic_port>/test`
- Логи транзакций выводятся в DEBUG режиме

## OpenTelemetry Integration

### Модуль `otel`

Предоставляет интеграцию с OpenTelemetry через header-based propagation:

- **`OpenTelemetryMessageContextPropagator`** — инъекция/извлечение W3C trace headers (`traceparent`, `tracestate`)
- **`OpenTelemetryMessageConsumerTelemetry`** — создание consumer spans вокруг обработки сообщений

### Пример использования

```java
OpenTelemetry openTelemetry = GlobalOpenTelemetry.get();

var propagator = new OpenTelemetryMessageContextPropagator(openTelemetry);
var consumerTelemetry = new OpenTelemetryMessageConsumerTelemetry(openTelemetry);

Producer<OrderCreated> producer = queueManager.registerProducer(
        ProducerConfig.<OrderCreated>builder()
                .queueName(queueName)
                .clazz(OrderCreated.class)
                .messageContextPropagator(propagator)
                .build()
);

queueManager.registerConsumer(
        ConsumerConfig.<OrderCreated>builder()
                .queueName(queueName)
                .subscriptionId(subscriptionId)
                .messageHandler(message -> handle(message.payload()))
                .messageContextPropagator(propagator)
                .messageConsumerTelemetry(consumerTelemetry)
                .build()
);
```

### Поведение

- Producer injects `traceparent` и связанные headers в сообщение
- Consumer извлекает контекст из headers сообщения
- Если trace headers отсутствуют — создаётся новый root span
- Consumer tracing записывает `traceId` и `spanId` в MDC во время обработки

## PostgreSQL Partition Management

### Автоматическое создание партиций

`PgQueryService` обеспечивает создание требуемых партиций перед операциями записи:

- Queue partition — перед вставкой сообщения
- History partition — перед complete/fail (если history включён)

Создание защищено `pg_advisory_xact_lock(...)`.

### Управление через `PgTableManager`

```java
PgTableManager pgTableManager = new PgTableManager(
        queryService,
        "0 0 1 * * ?",  // создание будущих партиций
        "0 0 2 * * ?"   // очистка старых партиций
);

QueueManager queueManager = new QueueManager(queryService, transactionService, pgTableManager);

queueManager.registerQueue(
        QueueConfig.builder()
                .queueName(queueName)
                .properties(QueueConfig.Properties.builder()
                        .retentionDays(30)
                        .build())
                .build()
);

pgTableManager.startCronJobs();
```

### Формат имён партиций

```
<table>_yyyy_MM_dd
```

Примеры:
- `test_message_2026_04_17`
- `orders_history_2026_04_18`

Границы партиций всегда по UTC.

## Quick Start Example

### 1. Регистрация payload типов

```java
JsonbConverter jsonbConverter = new JsonbConverter();
jsonbConverter.registerType("order-created", OrderCreated.class);
```

### 2. Создание инфраструктуры

```java
QueueName queueName = new QueueName("orders");
SubscriptionId subscriptionId = new SubscriptionId("billing");

JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

SpringPersistenceService persistenceService = new SpringPersistenceService(jdbcTemplate);

PgQueryService queryService = new PgQueryService(
        persistenceService, 
        new SchemaName("public"), 
        jsonbConverter
);

SpringTransactionService transactionService = 
        new SpringTransactionService(new TransactionTemplate(new JdbcTransactionManager(dataSource)));

QueueManager queueManager = new QueueManager(queryService, transactionService);
```

### 3. Создание таблиц (DDL)

```bash
java -jar pg-cli/target/pg-cli-1.0.0.jar all \
  --schema public \
  --queue orders \
  --subscription billing \
  --history-enabled > dbq-orders.sql
```

### 4. Регистрация очереди, producer, consumer

```java
// Регистрация очереди
queueManager.registerQueue(
        QueueConfig.builder()
                .queueName(queueName)
                .build()
);

// Регистрация producer
Producer<OrderCreated> producer = queueManager.registerProducer(
        ProducerConfig.<OrderCreated>builder()
                .queueName(queueName)
                .clazz(OrderCreated.class)
                .build()
);

// Регистрация consumer
queueManager.registerConsumer(
        ConsumerConfig.<OrderCreated>builder()
                .queueName(queueName)
                .subscriptionId(subscriptionId)
                .messageHandler(message -> {
                    OrderCreated payload = message.payload();
                    System.out.println("Received order " + payload.orderId());
                })
                .properties(ConsumerConfig.Properties.builder()
                        .concurrency(4)
                        .maxPollRecords(1)
                        .historyEnabled(true)
                        .build())
                .build()
);

// Запуск потребителей
queueManager.startConsumers();

// Публикация сообщения
producer.send(new OrderCreated("order-123"));

// Остановка (on shutdown)
queueManager.stopConsumers();
```

## Troubleshooting

### Тесты не запускаются

**Проблема:** Integration-тесты падают с ошибкой подключения к Docker

**Решение:**
```bash
# Проверить статус Docker
docker ps

# Перезапустить Docker Desktop
# Убедиться что Testcontainers может тянуть образы
docker pull postgres:18-alpine
docker pull ghcr.io/shopify/toxiproxy:2.5.0
```

### Ошибки компиляции

**Проблема:** Lombok не генерирует методы

**Решение:**
```bash
# Очистить и пересобрать
mvn -q clean compile

# Проверить что annotation processor включён в pom.xml
```

### Проблемы с партициями

**Проблема:** Partition already exists или wrong range

**Решение:**
- DBQ автоматически детектирует некорректные партиции и падает с ошибкой
- Проверить что `retentionDays` настроен корректно
- Убедиться что время на сервере синхронизировано (партиции по UTC)

### Consumer не потребляет сообщения

**Чеклист:**
1. Очередь зарегистрирована через `registerQueue(...)`?
2. Consumer зарегистрирован до `startConsumers()`?
3. Подписка создана в БД (таблица `<subscription>`)?
4. Нет ли blocking исключений в логах?
5. Проверить `execute_after` — сообщение может быть запланировано на будущее

## Additional Resources

- **DOCUMENTATION.MD** — полная архитектура, ER диаграммы, sequence diagrams
- **README.MD** — quick start и базовое описание API
- **PleaseWriteTest.MD** — рекомендации по покрытию тестами
- **issues.md** — отслеживаемые проблемы
- **verified-hypotheses.md** — проверенные гипотезы и решения
