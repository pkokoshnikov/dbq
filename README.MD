# Message bus

## Description
This library is intended for supporting of **Outbox pattern** in general. And it can be used for publishing/subscribing of 
domain events in eventual consistency manner. But of course it can be used as a simple queue on your db.

### The library supports:
* publishing messages (single and batch)
* consuming with multiple subscriptions (single and batch)
* retrying of failed messages
* blocking subscription processing in case of unavailable resources 
* PostgreSQL CLI tool for generating message and subscription DDL
* optional PostgreSQL partition management and cleanup

## Usage
### Initialization
First of all you need to initialize the queue, it should be one instance per application. It can be a singleton bean in Spring.
```java
    // init queue
    PersistenceService persistenceService = new SpringPersistenceService(jdbcTemplate);
    QueryService queryService = new PgQueryService(persistenceService, new SchemaName("schema"), jsonbConverter);
    TransactionService transactionService = new SpringTransactionService(new TransactionTemplate(new JdbcTransactionManager(dataSource) {}));
    QueueManager queue = new QueueManager(
            queryService,
            transactionService,
            new StdMessageFactory());
```

### Schema and partitions
`QueueManager` no longer creates tables or manages partitions automatically. Applications should create
message and subscription tables via migrations.

For PostgreSQL you can generate the required DDL programmatically:
```java
    PgSchemaSqlGenerator sqlGenerator = new PgSchemaSqlGenerator(new SchemaName("public"));

    String messageSql = sqlGenerator.createQueueTable(QUEUE_NAME);
    String subscriptionSql = sqlGenerator.createSubscriptionTable(QUEUE_NAME, SUBSCRIPTION_NAME);
```

The same generator is available through a dedicated CLI tool:
```bash
    java -cp pg/target/pg-0.0.1.jar org.pak.messagebus.pg.cli.PgSchemaCli queue public test-message
    java -cp pg/target/pg-0.0.1.jar org.pak.messagebus.pg.cli.PgSchemaCli subscription public test-message test-subscription
    java -cp pg/target/pg-0.0.1.jar org.pak.messagebus.pg.cli.PgSchemaCli all public test-message test-subscription
```

If you want automatic daily partition creation and cleanup for PostgreSQL, run a dedicated pg-specific manager explicitly:
```java
    PgTableManager pgTableManager = new PgTableManager(
            new PgQueryService(persistenceService, new SchemaName("schema"), jsonbConverter),
            "0 0 1 * * ?",
            "0 0 2 * * ?");

    // parent tables should already exist via migrations
    pgTableManager.registerQueue(QUEUE_NAME, 30);
    pgTableManager.registerSubscription(QUEUE_NAME, SUBSCRIPTION_NAME, 30);
    pgTableManager.startCronJobs();
```
### Publishing 
#### Registration
To start publishing messages you need to register producers, it consumes producer config with queue name and class.
Queue name is a unique identifier of queue.
```java
    // register producer
    var producer = queue.registerProducer(ProducerConfig.<Message>builder()
        .queueName(QUEUE_NAME)
        .clazz(Message.class)
        .build());
```
#### Sending
Message class should be serializable to JSON, because it will be serialized and stored in db.
To publish message you need to call send method on producer with message payload.
```java
    // publish message
    producer.send(new Message("test"));
```
### Subscribing
#### Registration
To start consuming messages you need to register a consumer config with queue name and subscription id.
Subscription id is a unique identifier of subscription. Besides you can specify properties for message handling, for example concurrency.
The most important property is messageHandler where your business logic should be placed.
```java
    // register consumer
    queue.registerConsumer(ConsumerConfig.<TestMessage>builder()
        .messageHandler(message -> { // your business logic
            System.out.println("Message received: " + message);
        })
        .queueName(QUEUE_NAME) // unique identifier of queue
        .subscriptionId(SUBSCRIPTION_ID) // unique identifier of subscription group
        .properties(ConsumerConfig.Properties.builder()
            .concurrency(10) // number of threads for consuming messages
            .build())
        .build());
```
#### Starting
To start consuming messages you need to call `startConsumers` method.
```java
    // start consumers
    queue.startConsumers();
```
#### Stopping
To stop consuming messages you need to call `stopConsumers` method.
```java
    // stop consumers
    queue.stopConsumers();
```

## Abstraction layers:
The library support abstraction layers for:
* database queries layer - it can be used with any database that supports SQL that support select for update skip locked,
  default impl is for `PostgreSQL`
* persistence layer - it can be used with any frameworks that supports jdbc operations, default impl is for Spring
* tracing propagation layer - core exposes message headers and propagator hooks, and `otel` module provides OpenTelemetry adapter

### OpenTelemetry
If you use OpenTelemetry, prefer message headers propagation instead of extracting trace id from payload.
You can attach the provided adapter on producer and consumer config:
```java
    var propagator = new OpenTelemetryMessageContextPropagator();

    var producer = queue.registerProducer(ProducerConfig.<Message>builder()
        .queueName(QUEUE_NAME)
        .clazz(Message.class)
        .messageContextPropagator(propagator)
        .build());

    queue.registerConsumer(ConsumerConfig.<Message>builder()
        .messageHandler(message -> System.out.println(message.payload()))
        .queueName(QUEUE_NAME)
        .subscriptionId(SUBSCRIPTION_ID)
        .messageContextPropagator(propagator)
        .build());
```

## Outbox Pattern
### Publishing
You should always publish your messages(events) in the same transaction with your business logic. Only in this case you can be 
sure that your message will be published consensually with main logic.
### Subscribing
When messageHandler processes a message, it starts transaction and you are not necessary to start new transaction. It gives you guarantee
that your business logic changes will be committed with correct message subscription status(`PROCESSED` or `FAILED`).
## Consumer properties 
### maxPollRecords
Please be cautious when configuring the maxPollRecords property, as a transaction is initiated for all messages polled 
from the database in batch. If you have a message handler with a long running time, this can lead to extended transaction durations.
It is advisable not to change the `maxPollRecords` value unless you are certain it is necessary for your specific case.
A large `maxPollRecords` value can be useful when moving messages from DB to MB. However, it's important to consider the 
impact on transaction duration and overall system performance before adjusting this value.
It's always recommended to evaluate your specific use case and consider the potential implications before making any 
changes to the `maxPollRecords` property or any other configuration settings related to message processing.
### concurrency
The `concurrency` property sets the number of message handlers that work simultaneously. If you don't have a high load, 
it is not necessary to change it.
### storageDays
The storageDays property is used by PostgreSQL partition management. It sets the maximum number of days to retain event
data. However, it does not guarantee that partitions will be deleted exactly after the specified date. If there are
unprocessed messages in a partition, they will not be deleted.
### persistenceExceptionPause
The `persistenceExceptionPause` sets the pause duration if a persistence exception occurs. This pause allows for resolving 
the persistence issue before resuming normal operations.
### unpredictedExceptionPause
The same for unpredicted exception.

## Retry
During processing of message you can throw exception, in this case message subscription will be marked as `FAILED` or retried. 
You can configure retryable policy, blocking policy, and non retrayble policy for each messageHandler. Please see ConsumerConfig.
### Retryable policy
It consumes Exception and attempt and returns duration. Please see `SimpleRetryablePolicy` for details. All your exceptions
by default is retryable. If you want to mark some exceptions as non retryable you should use `NonRetryablePolicy`.
### Non retryable policy
It consumes Exception and returns boolean. If it returns true message will be marked as `FAILED` at once.
### Blocking policy
It consumes attempt and returns duration. You can use it for blocking message processing in case of unavailable your 
resources. In this case message will be retried after duration and attempt will not be incremented.
