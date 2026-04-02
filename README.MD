# Message bus

## Description
This library is intended for supporting of **Outbox pattern** in general. And it can be used for publishing/subscribing of 
domain events in eventual consistency manner. But of course it can be used as a simple queue on your db.

### The library supports:
* publishing messages (single and batch)
* consuming with multiple subscriptions (single and batch)
* retrying of failed messages
* blocking subscription processing in case of unavailable resources 
* PostgreSQL SQL generation for message and subscription tables
* optional PostgreSQL partition management and cleanup

## Usage
### Initialization
First of all you need to initialize the message bus, it should be one instance per application. It can be a singleton bean in Spring.
```java
    //init message bus
    PersistenceService persistenceService = new SpringPersistenceService(jdbcTemplate);
    QueryService queryService = new PgQueryService(persistenceService, new SchemaName("schema"), jsonbConverter);
    TransactionService transactionService = new SpringTransactionService(new TransactionTemplate(new JdbcTransactionManager(dataSource) {}));
    MessageBus messageBus = new MessageBus(
            queryService,
            transactionService,
            new StdMessageFactory());
```

### Schema and partitions
`MessageBus` and `QueueAdapter` no longer create tables or manage partitions automatically. Applications should create
message and subscription tables via migrations.

For PostgreSQL you can generate the required DDL:
```java
    PgSchemaSqlGenerator sqlGenerator = new PgSchemaSqlGenerator(new SchemaName("schema"));

    String messageTableSql = sqlGenerator.createMessageTable(MESSAGE_NAME);
    String subscriptionTableSql = sqlGenerator.createSubscriptionTable(MESSAGE_NAME, SUBSCRIPTION_NAME);
```

If you want automatic daily partition creation and cleanup for PostgreSQL, run a dedicated pg-specific manager explicitly:
```java
    PgTableManager pgTableManager = new PgTableManager(
            new PgQueryService(persistenceService, new SchemaName("schema"), jsonbConverter),
            "0 0 1 * * ?",
            "0 0 2 * * ?");

    pgTableManager.registerMessage(MESSAGE_NAME, 30);
    pgTableManager.registerSubscription(MESSAGE_NAME, SUBSCRIPTION_NAME, 30);
    pgTableManager.startCronJobs();
```
### Publishing 
#### Registration
To start publishing messages you need to register publishers, it consumes publisher config with message name and class. 
Message name is a unique identifier of message type.
```java
    //register publisher 
    messageBus.registerPublisher(PublisherConfig.<Message>builder()
        .messageName(MESSAGE_NAME)
        .clazz(Message.class)
        .build());
```
#### Sending
Message class should be serializable to JSON, because it will be serialized and stored in db.
To publish message you need to call publish method with message name and message.
```java
    //publish message
    messageBus.publish(MESSAGE_NAME, new Message("test"));
```
### Subscribing
#### Registration
To start consuming messages you need to register subscribers, it consumes subscriber config with message name and subscription name. 
Subscription name is a unique identifier of subscriber. Besides you can specify properties for subscriber, for example concurrency. 
The most important property is listener where should be placed your business logic.
```java
    //register subscriber
    messageBus.registerSubscriber(SubscriberConfig.<TestMessage>builder()
        .messageListener(message -> { // your business logic
            System.out.println("Message received: " + message);
        })
        .messageName(MESSAGE_NAME) // unique identifier of message type
        .subscriptionName(SUBSCRIPTION_NAME) // unique identifier of subscription group
        .properties(SubscriberConfig.Properties.builder()
            .concurrency(10) // number of threads for consuming messages
            .build())
        .build());
```
#### Starting
To start consuming messages you need to call `startSubscribers` method.
```java
    //start subscribers
    messageBus.startSubscribers();
```
#### Stopping
To stop consuming messages you need to call `stopSubscribers` method.
```java
    //stop subscribers
    messageBus.stopSubscribers();
```

## Abstraction layers:
The library support abstraction layers for:
* database queries layer - it can be used with any database that supports SQL that support select for update skip locked,
  default impl is for `PostgreSQL`
* persistence layer - it can be used with any frameworks that supports jdbc operations, default impl is for Spring

## Outbox Pattern
### Publishing
You should always publish your messages(events) in the same transaction with your business logic. Only in this case you can be 
sure that your message will be published consensually with main logic.
### Subscribing
When subscriber process message, it starts transaction and you are not necessary to start new transaction. It gives you guarantee 
that your business logic changes will be committed with correct message subscription status(`PROCESSED` or `FAILED`).
### Message brokers
If your application has two or more services, you can use message brokers to move events from one service to another. 
To achieve this, you should use a `QueueAdapter`. It provides the possibility to move messages in batches from a database 
(DB) to a message broker (MB), and then from the MB back to the DB. Each message has a unique key that is set 
when the message is stored in the DB. When sending messages via the MB, you don't need to worry about duplicate messages, 
as any duplicates will be skipped during saving messages from MB to DB. However, it is important to receive confirmation 
from the MB that the messages have been successfully saved during sending messages the MB from DB. And you should acknowledge messages 
only when they have been safely stored in the DB during saving messages from MB to DB. Only in this case you can ensure the 
messages are handled safely. Please note that there may be further context or specific requirements depending on 
the technology or framework you are using. The above information is based on the general principles of using message 
brokers for event-driven architectures.

## Subscriber properties 
### maxPollRecords
Please be cautious when configuring the maxPollRecords property, as a transaction is initiated for all messages polled 
from the database in batch. If you have a message listener with a long running time, this can lead to extended transaction durations. 
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
You can configure retryable policy, blocking policy, and non retrayble policy for each subscriber. Please see SubscriberConfig.
### Retryable policy
It consumes Exception and attempt and returns duration. Please see `StdRetryablePolicy` for details. All your exceptions 
by default is retryable. If you want to mark some exceptions as non retryable you should use `NonRetryablePolicy`.
### Non retryable policy
It consumes Exception and returns boolean. If it returns true message will be marked as `FAILED` at once.
### Blocking policy
It consumes attempt and returns duration. You can use it for blocking message processing in case of unavailable your 
resources. In this case message will be retried after duration and attempt will not be incremented.
