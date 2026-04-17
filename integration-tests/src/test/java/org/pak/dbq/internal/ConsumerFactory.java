package org.pak.dbq.internal;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.experimental.FieldDefaults;
import org.pak.dbq.api.*;
import org.pak.dbq.api.policy.BlockingPolicy;
import org.pak.dbq.api.policy.NonRetryablePolicy;
import org.pak.dbq.api.policy.RetryablePolicy;
import org.pak.dbq.internal.consumer.AbstractConsumer;
import org.pak.dbq.internal.consumer.BatchConsumer;
import org.pak.dbq.internal.consumer.Consumer;
import org.pak.dbq.spi.*;

@Builder(toBuilder = true)
@FieldDefaults(level = AccessLevel.PRIVATE)
public class ConsumerFactory<T> {
    MessageHandler<T> messageHandler;
    BatchMessageHandler<T> batchMessageHandler;
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

    public AbstractConsumer<T> create() {
        if (batchMessageHandler != null) {
            return new BatchConsumer<>(batchMessageHandler, queueName, subscriptionId, queryService,
                    transactionService, messageContextPropagator, messageConsumerTelemetry, messageFactory, properties);
        }

        return new Consumer<>(messageHandler, queueName, subscriptionId, retryablePolicy,
                nonRetryablePolicy, blockingPolicy, queryService, transactionService, messageContextPropagator,
                messageConsumerTelemetry, messageFactory, properties);
    }
}
