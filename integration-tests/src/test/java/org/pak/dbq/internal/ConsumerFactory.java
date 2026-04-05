package org.pak.dbq.internal;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.experimental.FieldDefaults;
import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.MessageHandler;
import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.api.policy.BlockingPolicy;
import org.pak.dbq.api.policy.NonRetryablePolicy;
import org.pak.dbq.api.policy.RetryablePolicy;
import org.pak.dbq.internal.consumer.Consumer;
import org.pak.dbq.spi.*;

@Builder(toBuilder = true)
@FieldDefaults(level = AccessLevel.PRIVATE)
public class ConsumerFactory<T> {
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

    public Consumer<T> create() {
        return new Consumer<>(messageHandler, queueName, subscriptionId, retryablePolicy,
                nonRetryablePolicy, blockingPolicy, queryService, transactionService, messageContextPropagator,
                messageConsumerTelemetry, messageFactory, properties);
    }
}
