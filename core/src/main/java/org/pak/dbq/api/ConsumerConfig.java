package org.pak.dbq.api;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.FieldDefaults;
import org.pak.dbq.api.policy.*;
import org.pak.dbq.internal.support.NoOpMessageConsumerTelemetry;
import org.pak.dbq.internal.support.NoOpMessageContextPropagator;
import org.pak.dbq.spi.MessageConsumerTelemetry;
import org.pak.dbq.spi.MessageContextPropagator;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

@FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
@Getter
public class ConsumerConfig<T> {
    @NonNull
    QueueName queueName;
    SubscriptionId subscriptionId;
    MessageHandler<T> messageHandler;
    BatchMessageHandler<T> batchMessageHandler;
    BlockingPolicy blockingPolicy;
    RetryablePolicy retryablePolicy;
    NonRetryablePolicy nonRetryablePolicy;
    Properties properties;
    MessageContextPropagator messageContextPropagator;
    MessageConsumerTelemetry messageConsumerTelemetry;

    @Builder
    public ConsumerConfig(
            @NonNull QueueName queueName,
            @NonNull SubscriptionId subscriptionId,
            MessageHandler<T> messageHandler,
            BatchMessageHandler<T> batchMessageHandler,
            BlockingPolicy blockingPolicy,
            RetryablePolicy retryablePolicy,
            NonRetryablePolicy nonRetryablePolicy,
            Properties properties,
            MessageContextPropagator messageContextPropagator,
            MessageConsumerTelemetry messageConsumerTelemetry
    ) {
        this.queueName = Objects.requireNonNull(queueName, "queueName");
        this.subscriptionId = Objects.requireNonNull(subscriptionId, "subscriptionId");
        validateHandlers(messageHandler, batchMessageHandler);
        this.messageHandler = messageHandler;
        this.batchMessageHandler = batchMessageHandler;
        this.blockingPolicy = blockingPolicy != null ? blockingPolicy : new SimpleBlockingPolicy();
        this.retryablePolicy = retryablePolicy != null ? retryablePolicy : new SimpleRetryablePolicy();
        this.nonRetryablePolicy = nonRetryablePolicy != null ? nonRetryablePolicy : new SimpleNonRetryablePolicy();
        this.properties = properties != null ? properties : Properties.builder().build();
        this.messageContextPropagator = messageContextPropagator != null
                ? messageContextPropagator
                : new NoOpMessageContextPropagator();
        this.messageConsumerTelemetry = messageConsumerTelemetry != null
                ? messageConsumerTelemetry
                : new NoOpMessageConsumerTelemetry();
    }

    private void validateHandlers(MessageHandler<T> messageHandler, BatchMessageHandler<T> batchMessageHandler) {
        if ((messageHandler == null) == (batchMessageHandler == null)) {
            throw new IllegalArgumentException(
                    "Exactly one of messageHandler or batchMessageHandler must be configured");
        }
    }

    @Getter
    @FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
    public static class Properties {
        Integer maxPollRecords;
        Integer concurrency;
        Duration persistenceExceptionPause;
        Duration unpredictedExceptionPause;
        boolean historyEnabled;
        boolean serializedByKey;

        @Builder
        public Properties(
                Integer maxPollRecords,
                Integer concurrency,
                Duration persistenceExceptionPause,
                Duration unpredictedExceptionPause,
                boolean historyEnabled,
                boolean serializedByKey
        ) {
            this.maxPollRecords = maxPollRecords != null ? maxPollRecords : 1;
            this.concurrency = concurrency != null ? concurrency : 1;
            this.persistenceExceptionPause = persistenceExceptionPause != null
                    ? persistenceExceptionPause
                    : Duration.of(30, ChronoUnit.SECONDS);
            this.unpredictedExceptionPause = unpredictedExceptionPause != null
                    ? unpredictedExceptionPause
                    : Duration.of(30, ChronoUnit.SECONDS);
            this.historyEnabled = historyEnabled;
            this.serializedByKey = serializedByKey;

            validate();
        }

        private void validate() {
            if (maxPollRecords <= 0) {
                throw new IllegalArgumentException("maxPollRecords must be > 0");
            }
            if (concurrency <= 0) {
                throw new IllegalArgumentException("concurrency must be > 0");
            }
            if (persistenceExceptionPause.isNegative()) {
                throw new IllegalArgumentException("persistenceExceptionPause must be >= 0");
            }
            if (unpredictedExceptionPause.isNegative()) {
                throw new IllegalArgumentException("unpredictedExceptionPause must be >= 0");
            }
        }
    }
}
