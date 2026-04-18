# DBQ

DBQ is a database-backed queue, which provides multiple independent subscriptions. It can be used for transactional 
outbox-style publishing and subscription processing.

The project is built around a simple idea: store messages in DB, let independent subscriptions process them in their own 
transactions, and keep the API small enough to embed into an application without bringing a full broker.

Detailed architecture, table model, ER diagram, and producer/consumer interaction diagrams are documented in
[`documentation.md`](DOCUMENTATION.MD).

## What It Provides

- transactional message publishing against your application database
- multiple independent subscriptions for the same queue
- retry, blocking, and non-retryable failure policies
- PostgreSQL DDL generation for queue and subscription tables
- optional PostgreSQL partition creation and cleanup
- optional OpenTelemetry context propagation and consumer spans

## Modules

- `core` - queue API, producer/consumer runtime, policies, message model
- `pg` - PostgreSQL `QueryService`, JSONB conversion, DDL generator, partition manager
- `spring` - Spring JDBC persistence adapter and Spring transaction adapter
- `pg-cli` - standalone CLI for generating PostgreSQL DDL
- `otel` - OpenTelemetry propagation and consumer tracing integration
- `integration-tests` - end-to-end tests with PostgreSQL and Testcontainers

## Requirements

- Java 25
- PostgreSQL for the provided storage implementation
- queue names and subscription ids must use lowercase letters and `-`
- schema names must use lowercase letters and `_`

## Core Concepts

- `QueueName` identifies a logical queue, for example `orders`
- `SubscriptionId` identifies an independent consumer group, for example `billing`
- `Producer<T>` publishes messages into a queue
- `Consumer<T>` consumes messages from a queue
- `MessageHandler<T>` provides message handling
- `Message<T>` contains:
  - `key`
  - `originatedTime`
  - `payload`
  - `headers`
- `QueueManager` wires producers and consumers on top of `QueryService` and `TransactionService`

Each subscription has its own state. If two subscriptions are registered for the same queue, both will receive the same message and track their progress independently.

## Exception Model

DBQ uses `DbqException` as the checked root for infrastructure and conversion failures exposed by the library.

Top-level decision types:

- `RetryableDbqException` - caller may retry the operation
- `NonRetryableDbqException` - caller should treat the failure as terminal

Current concrete DBQ exceptions include:

- `RetryablePersistenceException`
- `NonRetryablePersistenceException`
- `MessageSerializationException`
- `MessageDeserializationException`

Typical client-side handling:

```java
try {
    producer.send(orderCreated);
} catch (RetryableDbqException e) {
    // retry, reschedule, or surface temporary failure
} catch (NonRetryableDbqException e) {
    // fail permanently and inspect concrete subtype for diagnostics
}
```

Notes:

- `MessageSerializationException` means DBQ could not serialize payload or headers before writing to PostgreSQL.
- `MessageDeserializationException` means DBQ could not reconstruct payload or headers from stored JSONB.
- `PersistenceException` is kept only as a deprecated compatibility type and should not be used as the main catch target in new code.

## Maven Coordinates

Add the modules you actually use:

```xml
<dependency>
    <groupId>org.pak</groupId>
    <artifactId>core</artifactId>
    <version>${dbq.version}</version>
</dependency>

<dependency>
    <groupId>org.pak</groupId>
    <artifactId>pg</artifactId>
    <version>${dbq.version}</version>
</dependency>

<dependency>
    <groupId>org.pak</groupId>
    <artifactId>spring</artifactId>
    <version>${dbq.version}</version>
</dependency>

<dependency>
    <groupId>org.pak</groupId>
    <artifactId>otel</artifactId>
    <version>${dbq.version}</version>
</dependency>
```

## Quick Start

### 1. Register payload types for JSONB

The PostgreSQL module stores payloads as JSONB. `JsonbConverter` must know how to deserialize them back.

Register every payload class under a unique type name:

```java
JsonbConverter jsonbConverter = new JsonbConverter();
jsonbConverter.registerType("order-created", OrderCreated.class);
```

If you use several payload classes, register each one explicitly.

### 2. Create infrastructure services

```java
QueueName queueName = new QueueName("orders");
SubscriptionId subscriptionId = new SubscriptionId("billing");

JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

SpringPersistenceService persistenceService =
        new SpringPersistenceService(jdbcTemplate);

PgQueryService queryService =
        new PgQueryService(persistenceService, new SchemaName("public"), jsonbConverter);

SpringTransactionService transactionService =
        new SpringTransactionService(new TransactionTemplate(new JdbcTransactionManager(dataSource)));

QueueManager queueManager = new QueueManager(queryService, transactionService);
```

### 3. Create database tables

`QueueManager` does not create tables automatically. Create them through migrations or generate SQL from the PostgreSQL module.

Programmatic generation:

```java
PgSchemaSqlGenerator sqlGenerator = new PgSchemaSqlGenerator(new SchemaName("public"));

String queueSql = sqlGenerator.createQueueTable(queueName);
String subscriptionSql = sqlGenerator.createSubscriptionTable(queueName, subscriptionId, true);
```

The third argument controls history support for that subscription:

- `true` - create the history table and keep processed / failed records
- `false` - create only the live subscription table; completed and failed messages are deleted from it

CLI generation:

```bash
mvn -q -pl pg-cli -am -DskipTests package

java -jar pg-cli/target/pg-cli-1.0.0.jar queue --schema public --queue orders
java -jar pg-cli/target/pg-cli-1.0.0.jar subscription --schema public --queue orders --subscription billing --history-enabled
java -jar pg-cli/target/pg-cli-1.0.0.jar all --schema public --queue orders --subscription billing --history-enabled
```

Examples:

```bash
# queue only
java -jar pg-cli/target/pg-cli-1.0.0.jar queue \
  --schema public \
  --queue orders

# subscription without history
java -jar pg-cli/target/pg-cli-1.0.0.jar subscription \
  --schema public \
  --queue orders \
  --subscription billing

# subscription with history
java -jar pg-cli/target/pg-cli-1.0.0.jar subscription \
  --schema public \
  --queue orders \
  --subscription billing \
  --history-enabled

# queue + subscription SQL into a file
java -jar pg-cli/target/pg-cli-1.0.0.jar all \
  --schema public \
  --queue orders \
  --subscription billing \
  --history-enabled > dbq-orders.sql
```

### 4. Register the queue

`QueueManager` requires explicit queue registration before producer or consumer registration.

```java
queueManager.registerQueue(
        QueueConfig.builder()
                .queueName(queueName)
                .build()
);
```

### 5. Register a producer

`registerProducer(...)` returns a `Producer<T>`. Keep that instance and use it directly.

```java
Producer<OrderCreated> producer = queueManager.registerProducer(
        ProducerConfig.<OrderCreated>builder()
                .queueName(queueName)
                .clazz(OrderCreated.class)
                .build()
);
```

If you enable history in runtime, the subscription schema should also be created with history support.

### 6. Register a consumer

```java
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
                        .serializedByKey(false)
                        .build())
                .build()
);
```

### 7. Start and stop consumers

```java
queueManager.startConsumers();

// on shutdown
queueManager.stopConsumers();
```

### 8. Register a batch consumer

Configure exactly one of `messageHandler(...)` or `batchMessageHandler(...)`.

```java
queueManager.registerConsumer(
        ConsumerConfig.<OrderCreated>builder()
                .queueName(queueName)
                .subscriptionId(subscriptionId)
                .batchMessageHandler((records, acknowledger) -> {
                    for (var record : records) {
                        OrderCreated payload = record.message().payload();
                        handle(payload);
                        acknowledger.complete(record);
                    }
                })
                .properties(ConsumerConfig.Properties.builder()
                        .maxPollRecords(50)
                        .historyEnabled(true)
                        .build())
                .build()
);
```

`MessageRecord<T>` contains:

- subscription record `id`
- original `Message<T>`
- current `attempt`
- current `executeAfter`

The batch contract is strict: every record returned to the batch handler must be acknowledged exactly once with
`complete(...)`, `retry(...)`, or `fail(...)`.

If you need per-key serialization inside one subscription, enable:

```java
ConsumerConfig.Properties.builder()
        .serializedByKey(true)
        .build()
```

With `serializedByKey=true`, DBQ uses a subscription-level key lock table so messages with the same `key` are not
polled concurrently by different consumer workers. The default is `false`, so ordinary mode keeps the previous polling
behavior and does not create key-lock DDL. If you switch an existing subscription to serialized mode, recreate that
subscription schema first.

### 9. Publish messages

The simplest form generates a random message key and current timestamp:

```java
producer.send(new OrderCreated("order-123"));
```

If you need your own deduplication key, timestamp, or headers, send a `Message<T>` explicitly:

```java
producer.send(new Message<>(
        "order-123",
        Instant.now(),
        new OrderCreated("order-123"),
        Map.of("tenant", "acme")
));
```

## Publishing Model

Messages are inserted into the queue table. Duplicate suppression is based on the pair `(key, originated_at)`.

That means:

- `producer.send(payload)` always creates a fresh message identity
- `producer.send(new Message<>(...))` lets you control deduplication

For outbox usage, call `producer.send(...)` inside the same database transaction as your business state change.

## Consumption Model

Each consumer subscription polls its own subscription table, locks rows with `FOR UPDATE SKIP LOCKED`, processes messages, and then:

- retries with a later `execute_after`
- if `historyEnabled=true`, moves completed / failed records to the history table with status `PROCESSED` or `FAILED`
- if `historyEnabled=false`, deletes completed / failed records from the live subscription table

The live subscription table is not partitioned. It stores only in-flight subscription state and rows are removed from it on `complete` / `fail`.

Queue retention is separate:

- queue messages stay in the queue table until old queue partitions are dropped
- if history is enabled, processed / failed records also stay in history until old history partitions are dropped

The message handler runs inside the consumer transaction. In practice this means your business-side database changes and the queue state transition are committed together.

Because delivery is retry-based, handlers should be idempotent.

## Message Headers

Headers are stored separately from the payload as JSONB.

Use headers for transport metadata such as:

- tracing context
- tenant identifiers
- correlation ids
- internal routing hints

Do not put observability metadata into the payload unless it is part of the business contract.

## Error Handling and Retry Policies

Consumer behavior is configurable per subscription through `ConsumerConfig`.

Defaults:

- `SimpleRetryablePolicy`
  - treats every exception as retryable by default
  - uses exponential backoff
  - caps the delay at 1 hour
  - returns `Optional.empty()` only when `attempt == Integer.MAX_VALUE`
- `SimpleNonRetryablePolicy`
  - marks nothing as non-retryable by default
- `SimpleBlockingPolicy`
  - marks nothing as blocking by default

In practice the default behavior is:

- any exception from `MessageHandler` goes through the retry flow
- the consumer keeps increasing `attempt` and rescheduling the message according to `SimpleRetryablePolicy`
- a message stops retrying only when the retry policy returns `Optional.empty()`
- with the default policy this happens only at `Integer.MAX_VALUE`

If you need a different behavior, you must configure it explicitly:

- use `NonRetryablePolicy` to fail specific exceptions immediately
- use `BlockingPolicy` to pause the whole consumer loop for infrastructure-style failures
- override `RetryablePolicy` if you need a different retry schedule or termination rule

Policy contracts:

- `RetryablePolicy`
  - returns `Optional<Duration>`
  - empty means "stop retrying and fail the message"
- `NonRetryablePolicy`
  - returns `true` when a message must fail immediately
- `BlockingPolicy`
  - returns a pause for the whole consumer loop without incrementing the attempt

Example:

```java
queueManager.registerConsumer(
        ConsumerConfig.<OrderCreated>builder()
                .queueName(queueName)
                .subscriptionId(subscriptionId)
                .messageHandler(message -> callRemoteService(message.payload()))
                .nonRetryablePolicy(exception -> exception instanceof IllegalArgumentException)
                .blockingPolicy(new BlockingPolicy() {
                    @Override
                    public boolean isBlocked(Exception exception) {
                        return exception instanceof ConnectException;
                    }

                    @Override
                    public Duration apply(Exception exception) {
                        return Duration.ofSeconds(30);
                    }
                })
                .retryablePolicy((exception, attempt) -> Optional.of(Duration.ofSeconds(5)))
                .build()
);
```

## Consumer Tuning

`ConsumerConfig.Properties` currently affects:

- `maxPollRecords` - how many messages are locked in one transaction
- `concurrency` - how many consumer threads are started for the subscription
- `persistenceExceptionPause` - delay after retryable persistence failures
- `unpredictedExceptionPause` - delay after unexpected consumer loop failures
- `historyEnabled` - whether completed / failed messages are written to subscription history

Guidance:

- keep `maxPollRecords` small unless you know why you need a larger batch
- increase `concurrency` only when the handler and downstream resources can handle parallelism

## OpenTelemetry

The `otel` module integrates DBQ with the new header-based propagation API.

Use two separate pieces:

- `OpenTelemetryMessageContextPropagator` for injecting and extracting W3C trace headers
- `OpenTelemetryMessageConsumerTelemetry` for starting a consumer span around message processing

Example:

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

Behavior:

- call `registerQueue(...)` before registering producers and consumers
- producer injects `traceparent` and related headers into the message
- consumer extracts context from message headers only
- if no trace headers are present, consumer processing starts a new root span
- consumer tracing writes `traceId` and `spanId` into MDC while the message is being processed

## PostgreSQL Partition Management

Partition management is optional.

If you want `QueueManager` registrations to automatically initialize partition retention, create `PgTableManager` and pass it into `QueueManager`:

```java
PgTableManager pgTableManager = new PgTableManager(
        queryService,
        "0 0 1 * * ?",
        "0 0 2 * * ?"
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

queueManager.registerConsumer(
        ConsumerConfig.<OrderCreated>builder()
                .queueName(queueName)
                .subscriptionId(subscriptionId)
                .messageHandler(message -> handle(message.payload()))
                .properties(ConsumerConfig.Properties.builder()
                        .historyEnabled(true)
                        .build())
                .build()
);

pgTableManager.startCronJobs();

// on shutdown
pgTableManager.stopCronJobs();
```

With this setup:

- `registerQueue(...)` creates the current and next queue partitions
- consumer registration with `historyEnabled=true` creates the current and next history partitions if the queue was registered
- creates future partitions on schedule
- drops old queue partitions when they are older than the configured retention window
- drops old history partitions only for subscriptions registered with `historyEnabled=true`

- `QueueConfig.Properties.retentionDays` controls partition retention for both queue and optional history partitions of that queue

The live subscription table is not cleaned by partition retention because it is not partitioned.

Parent tables must already exist before you start the manager.

## Build and Test

```bash
mvn -q -pl core test
mvn -q -pl otel -am test
mvn -q -pl integration-tests -am test -DskipTests
mvn -q -pl integration-tests -am test
```

The full integration suite requires Docker because it uses Testcontainers.
