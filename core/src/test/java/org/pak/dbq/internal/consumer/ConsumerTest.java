package org.pak.dbq.internal.consumer;

import org.junit.jupiter.api.Test;
import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.MessageHandler;
import org.pak.dbq.api.Message;
import org.pak.dbq.api.policy.BlockingPolicy;
import org.pak.dbq.api.policy.NonRetryablePolicy;
import org.pak.dbq.api.policy.RetryablePolicy;
import org.pak.dbq.error.MessageSerializationException;
import org.pak.dbq.internal.CoreTestSupport;
import org.pak.dbq.spi.MessageConsumerTelemetry;
import org.pak.dbq.spi.MessageContextPropagator;
import org.pak.dbq.error.NonRetrayablePersistenceException;
import org.pak.dbq.error.RetryablePersistenceException;
import org.pak.dbq.internal.support.NoOpMessageConsumerTelemetry;
import org.pak.dbq.internal.support.NoOpMessageContextPropagator;
import org.pak.dbq.internal.support.SimpleMessageFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pak.dbq.internal.CoreTestSupport.QUEUE_NAME;
import static org.pak.dbq.internal.CoreTestSupport.SUBSCRIPTION_NAME;
import static org.pak.dbq.internal.CoreTestSupport.messageContainer;

class ConsumerTest {
    @Test
    void poolAndProcessCompletesMessageWhenConsumerSucceeds() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var originatedTime = Instant.parse("2026-04-02T10:15:30Z");
        var headers = Map.of("traceparent", "00-test-parent");
        var container = CoreTestSupport.messageContainer("payload", headers, 0, originatedTime);
        queryService.setSelectedMessages(List.of(container));
        var handledMessage = new AtomicReference<Message<String>>();
        var messageContextPropagator = new CoreTestSupport.RecordingMessageContextPropagator(Map.of());
        var messageConsumerTelemetry = new CoreTestSupport.RecordingMessageConsumerTelemetry();
        var consumer = processor(
                queryService,
                transactionService,
                message -> handledMessage.set(message),
                blockingPolicy(false, Duration.ZERO),
                (exception, attempt) -> java.util.Optional.of(Duration.ofSeconds(5)),
                exception -> false,
                messageContextPropagator,
                messageConsumerTelemetry
        );

        var pooled = consumer.poolAndProcess();

        assertThat(pooled).isTrue();
        assertThat(queryService.getCompletions()).hasSize(1);
        assertThat(queryService.getCompletions().getFirst().historyEnabled()).isFalse();
        assertThat(queryService.getFailures()).isEmpty();
        assertThat(queryService.getRetries()).isEmpty();
        assertThat(handledMessage.get()).isEqualTo(new Message<>("key-1", originatedTime, "payload", headers));
        assertThat(messageContextPropagator.getExtractedHeaders()).isEqualTo(headers);
        assertThat(messageContextPropagator.isScopeClosed()).isTrue();
        assertThat(messageConsumerTelemetry.startedMessage()).isEqualTo(handledMessage.get());
        assertThat(messageConsumerTelemetry.startedQueueName()).isEqualTo(QUEUE_NAME);
        assertThat(messageConsumerTelemetry.startedSubscriptionId()).isEqualTo(SUBSCRIPTION_NAME);
        assertThat(messageConsumerTelemetry.recordedException()).isNull();
        assertThat(messageConsumerTelemetry.isScopeClosed()).isTrue();
    }

    @Test
    void poolAndProcessRetriesMessageWhenRetryPolicyReturnsDelay() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var container = CoreTestSupport.messageContainer("payload", 2, Instant.parse("2026-04-02T10:15:30Z"));
        queryService.setSelectedMessages(List.of(container));
        var expectedException = new IllegalStateException("retry");
        var consumer = processor(
                queryService,
                transactionService,
                message -> {
                    throw expectedException;
                },
                blockingPolicy(false, Duration.ZERO),
                (exception, attempt) -> java.util.Optional.of(Duration.ofSeconds(15)),
                exception -> false,
                new NoOpMessageContextPropagator(),
                new NoOpMessageConsumerTelemetry()
        );

        consumer.poolAndProcess();

        assertThat(queryService.getRetries()).hasSize(1);
        assertThat(queryService.getRetries().getFirst().retryDuration()).isEqualTo(Duration.ofSeconds(15));
        assertThat(queryService.getRetries().getFirst().exception()).isSameAs(expectedException);
        assertThat(queryService.getFailures()).isEmpty();
        assertThat(queryService.getCompletions()).isEmpty();
    }

    @Test
    void poolAndProcessFailsMessageWhenRetryPolicyStopsRetrying() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var container = CoreTestSupport.messageContainer("payload", 3, Instant.parse("2026-04-02T10:15:30Z"));
        queryService.setSelectedMessages(List.of(container));
        var expectedException = new IllegalStateException("fail");
        var messageConsumerTelemetry = new CoreTestSupport.RecordingMessageConsumerTelemetry();
        var processor = processor(
                queryService,
                transactionService,
                message -> {
                    throw expectedException;
                },
                blockingPolicy(false, Duration.ZERO),
                (exception, attempt) -> java.util.Optional.empty(),
                exception -> false,
                new NoOpMessageContextPropagator(),
                messageConsumerTelemetry
        );

        processor.poolAndProcess();

        assertThat(queryService.getFailures()).hasSize(1);
        assertThat(queryService.getFailures().getFirst().exception()).isSameAs(expectedException);
        assertThat(queryService.getFailures().getFirst().historyEnabled()).isFalse();
        assertThat(queryService.getRetries()).isEmpty();
        assertThat(queryService.getCompletions()).isEmpty();
        assertThat(messageConsumerTelemetry.recordedException()).isSameAs(expectedException);
        assertThat(messageConsumerTelemetry.isScopeClosed()).isTrue();
    }

    @Test
    void poolAndProcessFailsMessageWhenExceptionIsNonRetryable() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var container = CoreTestSupport.messageContainer("payload", 0, Instant.parse("2026-04-02T10:15:30Z"));
        queryService.setSelectedMessages(List.of(container));
        var expectedException = new IllegalArgumentException("non-retryable");
        var consumer = processor(
                queryService,
                transactionService,
                message -> {
                    throw expectedException;
                },
                blockingPolicy(false, Duration.ZERO),
                (exception, attempt) -> java.util.Optional.of(Duration.ofSeconds(10)),
                exception -> true,
                new NoOpMessageContextPropagator(),
                new NoOpMessageConsumerTelemetry()
        );

        consumer.poolAndProcess();

        assertThat(queryService.getFailures()).hasSize(1);
        assertThat(queryService.getFailures().getFirst().exception()).isSameAs(expectedException);
        assertThat(queryService.getFailures().getFirst().historyEnabled()).isFalse();
        assertThat(queryService.getRetries()).isEmpty();
        assertThat(queryService.getCompletions()).isEmpty();
    }

    @Test
    void poolAndProcessLeavesMessagePendingWhenExceptionIsBlocking() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var container = CoreTestSupport.messageContainer("payload", 0, Instant.parse("2026-04-02T10:15:30Z"));
        queryService.setSelectedMessages(List.of(container));
        var processor = processor(
                queryService,
                transactionService,
                message -> {
                    throw new IllegalStateException("blocked");
                },
                blockingPolicy(true, Duration.ofSeconds(5)),
                (exception, attempt) -> java.util.Optional.of(Duration.ofSeconds(10)),
                exception -> false,
                new NoOpMessageContextPropagator(),
                new NoOpMessageConsumerTelemetry()
        );

        processor.poolAndProcess();

        assertThat(queryService.getFailures()).isEmpty();
        assertThat(queryService.getRetries()).isEmpty();
        assertThat(queryService.getCompletions()).isEmpty();
    }

    @Test
    void poolAndProcessCompletesMessageWithHistoryWhenEnabled() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var container = CoreTestSupport.messageContainer("payload", 0, Instant.parse("2026-04-02T10:15:30Z"));
        queryService.setSelectedMessages(List.of(container));
        var consumer = processor(
                queryService,
                transactionService,
                message -> {
                },
                blockingPolicy(false, Duration.ZERO),
                (exception, attempt) -> java.util.Optional.of(Duration.ofSeconds(5)),
                exception -> false,
                new NoOpMessageContextPropagator(),
                new NoOpMessageConsumerTelemetry(),
                true
        );

        consumer.poolAndProcess();

        assertThat(queryService.getCompletions()).hasSize(1);
        assertThat(queryService.getCompletions().getFirst().historyEnabled()).isTrue();
    }

    @Test
    void poolAndProcessPassesSerializedByKeyFlagToQueryService() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var consumer = processor(
                queryService,
                transactionService,
                message -> {
                },
                blockingPolicy(false, Duration.ZERO),
                (exception, attempt) -> java.util.Optional.of(Duration.ofSeconds(5)),
                exception -> false,
                new NoOpMessageContextPropagator(),
                new NoOpMessageConsumerTelemetry(),
                false,
                true
        );

        var pooled = consumer.poolAndProcess();

        assertThat(pooled).isFalse();
        assertThat(queryService.isLastSerializedByKey()).isTrue();
    }

    @Test
    void poolAndProcessRethrowsMessageSerializationExceptionFromHandler() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var container = messageContainer("payload", 0, Instant.parse("2026-04-02T10:15:30Z"));
        queryService.setSelectedMessages(List.of(container));
        var exception = new MessageSerializationException(new IllegalStateException("boom"));
        var messageConsumerTelemetry = new CoreTestSupport.RecordingMessageConsumerTelemetry();
        var consumer = processor(
                queryService,
                transactionService,
                message -> {
                    throw exception;
                },
                blockingPolicy(false, Duration.ZERO),
                (handlerException, attempt) -> java.util.Optional.of(Duration.ofSeconds(5)),
                handlerException -> false,
                new NoOpMessageContextPropagator(),
                messageConsumerTelemetry
        );

        assertThatThrownBy(consumer::poolAndProcess).isSameAs(exception);
        assertThat(messageConsumerTelemetry.recordedException()).isSameAs(exception);
        assertThat(queryService.getFailures()).isEmpty();
        assertThat(queryService.getRetries()).isEmpty();
        assertThat(queryService.getCompletions()).isEmpty();
    }

    @Test
    void poolAndProcessRethrowsNonRetryablePersistenceExceptionFromHandler() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var container = messageContainer("payload", 0, Instant.parse("2026-04-02T10:15:30Z"));
        queryService.setSelectedMessages(List.of(container));
        var exception = new NonRetrayablePersistenceException(new RuntimeException("boom"), null);
        var messageConsumerTelemetry = new CoreTestSupport.RecordingMessageConsumerTelemetry();
        var consumer = processor(
                queryService,
                transactionService,
                message -> {
                    throw exception;
                },
                blockingPolicy(false, Duration.ZERO),
                (handlerException, attempt) -> java.util.Optional.of(Duration.ofSeconds(5)),
                handlerException -> false,
                new NoOpMessageContextPropagator(),
                messageConsumerTelemetry
        );

        assertThatThrownBy(consumer::poolAndProcess).isSameAs(exception);
        assertThat(messageConsumerTelemetry.recordedException()).isSameAs(exception);
        assertThat(queryService.getFailures()).isEmpty();
        assertThat(queryService.getRetries()).isEmpty();
        assertThat(queryService.getCompletions()).isEmpty();
    }

    @Test
    void poolAndProcessRethrowsRetryablePersistenceExceptionFromHandler() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var container = messageContainer("payload", 0, Instant.parse("2026-04-02T10:15:30Z"));
        queryService.setSelectedMessages(List.of(container));
        var exception = new RetryablePersistenceException(new RuntimeException("boom"), null);
        var messageConsumerTelemetry = new CoreTestSupport.RecordingMessageConsumerTelemetry();
        var consumer = processor(
                queryService,
                transactionService,
                message -> {
                    throw exception;
                },
                blockingPolicy(false, Duration.ZERO),
                (handlerException, attempt) -> java.util.Optional.of(Duration.ofSeconds(5)),
                handlerException -> false,
                new NoOpMessageContextPropagator(),
                messageConsumerTelemetry
        );

        assertThatThrownBy(consumer::poolAndProcess).isSameAs(exception);
        assertThat(messageConsumerTelemetry.recordedException()).isSameAs(exception);
        assertThat(queryService.getFailures()).isEmpty();
        assertThat(queryService.getRetries()).isEmpty();
        assertThat(queryService.getCompletions()).isEmpty();
    }

    @Test
    void poolAndProcessRethrowsInterruptedExceptionFromHandler() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var container = messageContainer("payload", 0, Instant.parse("2026-04-02T10:15:30Z"));
        queryService.setSelectedMessages(List.of(container));
        var exception = new InterruptedException("interrupted");
        var messageConsumerTelemetry = new CoreTestSupport.RecordingMessageConsumerTelemetry();
        var consumer = processor(
                queryService,
                transactionService,
                message -> sneakyThrow(exception),
                blockingPolicy(false, Duration.ZERO),
                (handlerException, attempt) -> java.util.Optional.of(Duration.ofSeconds(5)),
                handlerException -> false,
                new NoOpMessageContextPropagator(),
                messageConsumerTelemetry
        );

        assertThatThrownBy(consumer::poolAndProcess).isSameAs(exception);
        assertThat(messageConsumerTelemetry.recordedException()).isSameAs(exception);
        assertThat(queryService.getFailures()).isEmpty();
        assertThat(queryService.getRetries()).isEmpty();
        assertThat(queryService.getCompletions()).isEmpty();
    }

    private Consumer<String> processor(
            CoreTestSupport.RecordingQueryService queryService,
            CoreTestSupport.DirectTransactionService transactionService,
            MessageHandler<String> messageHandler,
            BlockingPolicy blockingPolicy,
            RetryablePolicy retryablePolicy,
            NonRetryablePolicy nonRetryablePolicy,
            MessageContextPropagator messageContextPropagator,
            MessageConsumerTelemetry messageConsumerTelemetry
    ) {
        return processor(queryService, transactionService, messageHandler, blockingPolicy, retryablePolicy,
                nonRetryablePolicy, messageContextPropagator, messageConsumerTelemetry, false);
    }

    private Consumer<String> processor(
            CoreTestSupport.RecordingQueryService queryService,
            CoreTestSupport.DirectTransactionService transactionService,
            MessageHandler<String> messageHandler,
            BlockingPolicy blockingPolicy,
            RetryablePolicy retryablePolicy,
            NonRetryablePolicy nonRetryablePolicy,
            MessageContextPropagator messageContextPropagator,
            MessageConsumerTelemetry messageConsumerTelemetry,
            boolean historyEnabled
    ) {
        return processor(queryService, transactionService, messageHandler, blockingPolicy, retryablePolicy,
                nonRetryablePolicy, messageContextPropagator, messageConsumerTelemetry, historyEnabled, false);
    }

    private Consumer<String> processor(
            CoreTestSupport.RecordingQueryService queryService,
            CoreTestSupport.DirectTransactionService transactionService,
            MessageHandler<String> messageHandler,
            BlockingPolicy blockingPolicy,
            RetryablePolicy retryablePolicy,
            NonRetryablePolicy nonRetryablePolicy,
            MessageContextPropagator messageContextPropagator,
            MessageConsumerTelemetry messageConsumerTelemetry,
            boolean historyEnabled,
            boolean serializedByKey
    ) {
        return new Consumer<>(
                messageHandler,
                QUEUE_NAME,
                SUBSCRIPTION_NAME,
                retryablePolicy,
                nonRetryablePolicy,
                blockingPolicy,
                queryService,
                transactionService,
                messageContextPropagator,
                messageConsumerTelemetry,
                new SimpleMessageFactory(),
                ConsumerConfig.Properties.builder()
                        .historyEnabled(historyEnabled)
                        .serializedByKey(serializedByKey)
                        .build()
        );
    }

    private BlockingPolicy blockingPolicy(boolean blocked, Duration pause) {
        return new BlockingPolicy() {
            @Override
            public boolean isBlocked(Exception exception) {
                return blocked;
            }

            @Override
            public Duration apply(Exception exception) {
                return pause;
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void sneakyThrow(Throwable throwable) throws E {
        throw (E) throwable;
    }
}
