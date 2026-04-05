package org.pak.dbq.api;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.FieldDefaults;
import org.pak.dbq.api.policy.BlockingPolicy;
import org.pak.dbq.api.policy.NonRetryablePolicy;
import org.pak.dbq.api.policy.RetryablePolicy;
import org.pak.dbq.api.policy.SimpleBlockingPolicy;
import org.pak.dbq.api.policy.SimpleNonRetryablePolicy;
import org.pak.dbq.api.policy.SimpleRetryablePolicy;
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
    @NonNull
    SubscriptionId subscriptionId;
    @NonNull
    MessageHandler<T> messageHandler;
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
            @NonNull MessageHandler<T> messageHandler,
            BlockingPolicy blockingPolicy,
            RetryablePolicy retryablePolicy,
            NonRetryablePolicy nonRetryablePolicy,
            Properties properties,
            MessageContextPropagator messageContextPropagator,
            MessageConsumerTelemetry messageConsumerTelemetry
    ) {
        this.queueName = Objects.requireNonNull(queueName, "queueName");
        this.subscriptionId = Objects.requireNonNull(subscriptionId, "subscriptionId");
        this.messageHandler = Objects.requireNonNull(messageHandler, "messageHandler");
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

    @Getter
    @FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
    public static class Properties {
        Integer maxPollRecords;
        Integer concurrency;
        Duration persistenceExceptionPause;
        Duration unpredictedExceptionPause;
        boolean historyEnabled;

        @Builder
        public Properties(
                Integer maxPollRecords,
                Integer concurrency,
                Duration persistenceExceptionPause,
                Duration unpredictedExceptionPause,
                boolean historyEnabled
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
