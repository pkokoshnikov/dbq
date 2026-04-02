package org.pak.messagebus.core;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.experimental.FieldDefaults;
import org.pak.messagebus.core.service.QueryService;
import org.pak.messagebus.core.service.TransactionService;

@Builder(toBuilder = true)
@FieldDefaults(level = AccessLevel.PRIVATE)
class QueueProcessorFactory<T> {
    Consumer<T> consumer;
    MessageFactory messageFactory;
    QueueName queueName;
    SubscriptionName subscriptionName;
    RetryablePolicy retryablePolicy;
    BlockingPolicy blockingPolicy;
    NonRetryablePolicy nonRetryablePolicy;
    QueryService queryService;
    TransactionService transactionService;
    TraceIdExtractor<T> traceIdExtractor;
    ConsumerConfig.Properties properties;

    QueueProcessor<T> create() {
        return new QueueProcessor<>(consumer, queueName, subscriptionName, retryablePolicy,
                nonRetryablePolicy, blockingPolicy, queryService, transactionService, traceIdExtractor,
                messageFactory, properties);
    }
}
