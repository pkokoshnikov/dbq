# Message Bus

Database-backed message bus for transactional outbox-style publishing and subscription processing.

The project is built around a simple idea: store messages in PostgreSQL, let independent subscriptions process them in their own transactions, and keep the API small enough to embed into an application without bringing a full broker.

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
- `MessageHandler<T>` processes messages for a subscription
- `Message<T>` contains:
  - `key`
  - `originatedTime`
  - `payload`
  - `headers`
- `QueueManager` wires producers and consumers on top of `QueryService` and `TransactionService`

Each subscription has its own state. If two subscriptions are registered for the same queue, both will receive the same message and track their progress independently.

## Maven Coordinates

Add the modules you actually use:

```xml
<dependency>
    <groupId>org.pak.messagebus</groupId>
    <artifactId>core</artifactId>
    <version>${messagebus.version}</version>
</dependency>

<dependency>
    <groupId>org.pak.messagebus</groupId>
    <artifactId>pg</artifactId>
    <version>${messagebus.version}</version>
</dependency>

<dependency>
    <groupId>org.pak.messagebus</groupId>
    <artifactId>spring</artifactId>
    <version>${messagebus.version}</version>
</dependency>

<dependency>
    <groupId>org.pak.messagebus</groupId>
    <artifactId>otel</artifactId>
    <version>${messagebus.version}</version>
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
String subscriptionSql = sqlGenerator.createSubscriptionTable(queueName, subscriptionId);
```

CLI generation:

```bash
java -cp pg/target/pg-0.0.1.jar org.pak.messagebus.pg.cli.PgSchemaCli queue public orders
java -cp pg/target/pg-0.0.1.jar org.pak.messagebus.pg.cli.PgSchemaCli subscription public orders billing
java -cp pg/target/pg-0.0.1.jar org.pak.messagebus.pg.cli.PgSchemaCli all public orders billing
```

### 4. Register a producer

`registerProducer(...)` returns a `Producer<T>`. Keep that instance and use it directly.

```java
Producer<OrderCreated> producer = queueManager.registerProducer(
        ProducerConfig.<OrderCreated>builder()
                .queueName(queueName)
                .clazz(OrderCreated.class)
                .build()
);
```

### 5. Register a consumer

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
                        .build())
                .build()
);
```

### 6. Start and stop consumers

```java
queueManager.startConsumers();

// on shutdown
queueManager.stopConsumers();
```

### 7. Publish messages

The simplest form generates a random message key and current timestamp:

```java
producer.send(new OrderCreated("order-123"));
```

If you need your own deduplication key, timestamp, or headers, send a `Message<T>` explicitly:

```java
producer.send(new SimpleMessage<>(
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
- `producer.send(new SimpleMessage<>(...))` lets you control deduplication

For outbox usage, call `producer.send(...)` inside the same database transaction as your business state change.

## Consumption Model

Each consumer subscription polls its own subscription table, locks rows with `FOR UPDATE SKIP LOCKED`, processes messages, and then marks them as:

- `PROCESSED`
- retried with a later `execute_after`
- `FAILED`

The message handler runs inside the consumer transaction. In practice this means your business-side database changes and the subscription status update are committed together.

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
  - exponential backoff
  - capped at 10 minutes
  - stops retrying after attempt `10000`
- `SimpleNonRetryablePolicy`
  - nothing is non-retryable by default
- `SimpleBlockingPolicy`
  - nothing is blocking by default

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

Guidance:

- keep `maxPollRecords` small unless you know why you need a larger batch
- increase `concurrency` only when the handler and downstream resources can handle parallelism

## OpenTelemetry

The `otel` module integrates with the new header-based propagation API.

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

- producer injects `traceparent` and related headers into the message
- consumer extracts context from message headers only
- if no trace headers are present, consumer processing starts a new root span
- consumer tracing writes `traceId` and `spanId` into MDC while the message is being processed

## PostgreSQL Partition Management

Partition management is optional and separate from `QueueManager`.

If you want automatic partition creation and cleanup, start `PgTableManager` explicitly:

```java
PgTableManager pgTableManager = new PgTableManager(
        queryService,
        "0 0 1 * * ?",
        "0 0 2 * * ?"
);

pgTableManager.registerQueue(queueName, 30);
pgTableManager.registerSubscription(queueName, subscriptionId, 30);
pgTableManager.startCronJobs();

// on shutdown
pgTableManager.stopCronJobs();
```

This manager:

- creates the current and next day partitions on registration
- creates future partitions on schedule
- drops old queue and history partitions when they are older than the configured retention window

Parent tables must already exist before you start the manager.

## Build and Test

```bash
mvn -q -pl core test
mvn -q -pl otel -am test
mvn -q -pl integration-tests -am test -DskipTests
mvn -q -pl integration-tests -am test
```

The full integration suite requires Docker because it uses Testcontainers.

## Notes

- the provided production storage implementation is PostgreSQL
- payloads and headers are persisted as JSONB
- `JsonbConverter` type registration is required before consuming stored payloads
- table creation is explicit; there is no hidden auto-migration in `QueueManager`
- use one queue per message contract unless you have a clear reason to mix heterogeneous payloads
