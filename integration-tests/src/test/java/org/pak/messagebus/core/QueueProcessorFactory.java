package org.pak.messagebus.core;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.experimental.FieldDefaults;
import org.pak.messagebus.core.service.QueryService;
import org.pak.messagebus.core.service.TransactionService;

@Builder(toBuilder = true)
@FieldDefaults(level = AccessLevel.PRIVATE)
class QueueProcessorFactory<T> {
    MessageHandler<T> messageHandler;
    MessageFactory messageFactory;
    QueueName queueName;
    SubscriptionId subscriptionId;
    RetryablePolicy retryablePolicy;
    BlockingPolicy blockingPolicy;
    NonRetryablePolicy nonRetryablePolicy;
    QueryService queryService;
    TransactionService transactionService;
    MessageContextPropagator messageContextPropagator;
    MessageConsumerTelemetry messageConsumerTelemetry;
    ConsumerConfig.Properties properties;

    Consumer<T> create() {
        return new Consumer<>(messageHandler, queueName, subscriptionId, retryablePolicy,
                nonRetryablePolicy, blockingPolicy, queryService, transactionService, messageContextPropagator,
                messageConsumerTelemetry, messageFactory, properties);
    }
}
