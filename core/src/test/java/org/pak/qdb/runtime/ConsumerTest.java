package org.pak.qdb.runtime;

import org.junit.jupiter.api.Test;
import org.pak.qdb.api.ConsumerConfig;
import org.pak.qdb.api.Message;
import org.pak.qdb.api.MessageHandler;
import org.pak.qdb.model.SimpleMessage;
import org.pak.qdb.policy.BlockingPolicy;
import org.pak.qdb.policy.NonRetryablePolicy;
import org.pak.qdb.policy.RetryablePolicy;
import org.pak.qdb.spi.MessageConsumerTelemetry;
import org.pak.qdb.spi.MessageContextPropagator;
import org.pak.qdb.support.NoOpMessageConsumerTelemetry;
import org.pak.qdb.support.NoOpMessageContextPropagator;
import org.pak.qdb.support.StdMessageFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.qdb.runtime.CoreTestSupport.QUEUE_NAME;
import static org.pak.qdb.runtime.CoreTestSupport.SUBSCRIPTION_NAME;
import static org.pak.qdb.runtime.CoreTestSupport.messageContainer;

class ConsumerTest {
    @Test
    void poolAndProcessCompletesMessageWhenConsumerSucceeds() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var originatedTime = Instant.parse("2026-04-02T10:15:30Z");
        var headers = Map.of("traceparent", "00-test-parent");
        var container = messageContainer("payload", headers, 0, originatedTime);
        queryService.selectedMessages = List.of(container);
        var handledMessage = new AtomicReference<Message<String>>();
        var messageContextPropagator = new CoreTestSupport.RecordingMessageContextPropagator(Map.of());
        var messageConsumerTelemetry = new CoreTestSupport.RecordingMessageConsumerTelemetry();
        var processor = processor(
                queryService,
                transactionService,
                message -> handledMessage.set(message),
                blockingPolicy(false, Duration.ZERO),
                (exception, attempt) -> java.util.Optional.of(Duration.ofSeconds(5)),
                exception -> false,
                messageContextPropagator,
                messageConsumerTelemetry
        );

        var pooled = processor.poolAndProcess();

        assertThat(pooled).isTrue();
        assertThat(queryService.completions).hasSize(1);
        assertThat(queryService.failures).isEmpty();
        assertThat(queryService.retries).isEmpty();
        assertThat(handledMessage.get()).isEqualTo(new SimpleMessage<>("key-1", originatedTime, "payload", headers));
        assertThat(messageContextPropagator.extractedHeaders()).isEqualTo(headers);
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
        var container = messageContainer("payload", 2, Instant.parse("2026-04-02T10:15:30Z"));
        queryService.selectedMessages = List.of(container);
        var expectedException = new IllegalStateException("retry");
        var processor = processor(
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

        processor.poolAndProcess();

        assertThat(queryService.retries).hasSize(1);
        assertThat(queryService.retries.getFirst().retryDuration()).isEqualTo(Duration.ofSeconds(15));
        assertThat(queryService.retries.getFirst().exception()).isSameAs(expectedException);
        assertThat(queryService.failures).isEmpty();
        assertThat(queryService.completions).isEmpty();
    }

    @Test
    void poolAndProcessFailsMessageWhenRetryPolicyStopsRetrying() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var container = messageContainer("payload", 3, Instant.parse("2026-04-02T10:15:30Z"));
        queryService.selectedMessages = List.of(container);
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

        assertThat(queryService.failures).hasSize(1);
        assertThat(queryService.failures.getFirst().exception()).isSameAs(expectedException);
        assertThat(queryService.retries).isEmpty();
        assertThat(queryService.completions).isEmpty();
        assertThat(messageConsumerTelemetry.recordedException()).isSameAs(expectedException);
        assertThat(messageConsumerTelemetry.isScopeClosed()).isTrue();
    }

    @Test
    void poolAndProcessFailsMessageWhenExceptionIsNonRetryable() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var container = messageContainer("payload", 0, Instant.parse("2026-04-02T10:15:30Z"));
        queryService.selectedMessages = List.of(container);
        var expectedException = new IllegalArgumentException("non-retryable");
        var processor = processor(
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

        processor.poolAndProcess();

        assertThat(queryService.failures).hasSize(1);
        assertThat(queryService.failures.getFirst().exception()).isSameAs(expectedException);
        assertThat(queryService.retries).isEmpty();
        assertThat(queryService.completions).isEmpty();
    }

    @Test
    void poolAndProcessLeavesMessagePendingWhenExceptionIsBlocking() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var container = messageContainer("payload", 0, Instant.parse("2026-04-02T10:15:30Z"));
        queryService.selectedMessages = List.of(container);
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

        assertThat(queryService.failures).isEmpty();
        assertThat(queryService.retries).isEmpty();
        assertThat(queryService.completions).isEmpty();
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
                new StdMessageFactory(),
                ConsumerConfig.Properties.builder().build()
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
}
