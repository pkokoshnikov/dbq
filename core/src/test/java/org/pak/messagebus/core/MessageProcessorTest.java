package org.pak.messagebus.core;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.messagebus.core.CoreTestSupport.MESSAGE_NAME;
import static org.pak.messagebus.core.CoreTestSupport.SUBSCRIPTION_NAME;
import static org.pak.messagebus.core.CoreTestSupport.messageContainer;

class MessageProcessorTest {
    @Test
    void poolAndProcessCompletesMessageWhenListenerSucceeds() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var originatedTime = Instant.parse("2026-04-02T10:15:30Z");
        var container = messageContainer("payload", 0, originatedTime);
        queryService.selectedMessages = List.of(container);
        var handledMessage = new AtomicReference<Message<String>>();
        var processor = processor(
                queryService,
                transactionService,
                message -> handledMessage.set(message),
                blockingPolicy(false, Duration.ZERO),
                (exception, attempt) -> java.util.Optional.of(Duration.ofSeconds(5)),
                exception -> false
        );

        var pooled = processor.poolAndProcess();

        assertThat(pooled).isTrue();
        assertThat(queryService.completions).hasSize(1);
        assertThat(queryService.failures).isEmpty();
        assertThat(queryService.retries).isEmpty();
        assertThat(handledMessage.get()).isEqualTo(new StdMessage<>("key-1", originatedTime, "payload"));
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
                exception -> false
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
        var processor = processor(
                queryService,
                transactionService,
                message -> {
                    throw expectedException;
                },
                blockingPolicy(false, Duration.ZERO),
                (exception, attempt) -> java.util.Optional.empty(),
                exception -> false
        );

        processor.poolAndProcess();

        assertThat(queryService.failures).hasSize(1);
        assertThat(queryService.failures.getFirst().exception()).isSameAs(expectedException);
        assertThat(queryService.retries).isEmpty();
        assertThat(queryService.completions).isEmpty();
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
                exception -> true
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
                exception -> false
        );

        processor.poolAndProcess();

        assertThat(queryService.failures).isEmpty();
        assertThat(queryService.retries).isEmpty();
        assertThat(queryService.completions).isEmpty();
    }

    private MessageProcessor<String> processor(
            CoreTestSupport.RecordingQueryService queryService,
            CoreTestSupport.DirectTransactionService transactionService,
            MessageListener<String> listener,
            BlockingPolicy blockingPolicy,
            RetryablePolicy retryablePolicy,
            NonRetryablePolicy nonRetryablePolicy
    ) {
        return new MessageProcessor<>(
                listener,
                MESSAGE_NAME,
                SUBSCRIPTION_NAME,
                retryablePolicy,
                nonRetryablePolicy,
                blockingPolicy,
                queryService,
                transactionService,
                payload -> "trace-id",
                new StdMessageFactory(),
                SubscriberConfig.Properties.builder().build()
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
