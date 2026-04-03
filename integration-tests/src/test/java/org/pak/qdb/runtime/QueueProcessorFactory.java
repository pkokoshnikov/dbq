package org.pak.qdb.runtime;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.experimental.FieldDefaults;
import org.pak.qdb.api.ConsumerConfig;
import org.pak.qdb.api.MessageHandler;
import org.pak.qdb.api.QueueName;
import org.pak.qdb.api.SubscriptionId;
import org.pak.qdb.policy.BlockingPolicy;
import org.pak.qdb.policy.NonRetryablePolicy;
import org.pak.qdb.policy.RetryablePolicy;
import org.pak.qdb.spi.*;

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
