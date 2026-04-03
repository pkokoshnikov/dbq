package org.pak.messagebus.core;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.FieldDefaults;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

@Builder
@FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
@Getter
public class ConsumerConfig<T> {
    @NonNull
    QueueName queueName;
    @NonNull
    SubscriptionId subscriptionId;
    @NonNull
    MessageHandler<T> messageHandler;
    @Builder.Default
    BlockingPolicy blockingPolicy = new SimpleBlockingPolicy();
    @Builder.Default
    RetryablePolicy retryablePolicy = new SimpleRetryablePolicy();
    @Builder.Default
    NonRetryablePolicy nonRetryablePolicy = new SimpleNonRetryablePolicy();
    @Builder.Default
    Properties properties = Properties.builder().build();
    @Builder.Default
    TraceIdExtractor<T> traceIdExtractor = new NullTraceIdExtractor<T>();

    @Builder
    @Getter
    @FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
    public static class Properties {
        //max number of records to poll from db
        @Builder.Default
        Integer maxPollRecords = 1;
        //max number of threads to process messages
        @Builder.Default
        Integer concurrency = 1;
        //pause between retries for retryable persistence exceptions
        @Builder.Default
        Duration persistenceExceptionPause = Duration.of(30, ChronoUnit.SECONDS);
        //pause between retries for unpredicted exceptions
        @Builder.Default
        Duration unpredictedExceptionPause = Duration.of(30, ChronoUnit.SECONDS);
        @Builder.Default
        int storageDays = 30;
    }
}
