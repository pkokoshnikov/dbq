package org.pak.dbq.internal.consumer;

import org.junit.jupiter.api.Test;
import org.pak.dbq.api.BatchMessageHandler;
import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.Message;
import org.pak.dbq.api.MessageRecord;
import org.pak.dbq.internal.CoreTestSupport;
import org.pak.dbq.internal.support.NoOpMessageConsumerTelemetry;
import org.pak.dbq.internal.support.NoOpMessageContextPropagator;
import org.pak.dbq.internal.support.SimpleMessageFactory;
import org.pak.dbq.spi.error.RetryablePersistenceException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pak.dbq.internal.CoreTestSupport.QUEUE_NAME;
import static org.pak.dbq.internal.CoreTestSupport.SUBSCRIPTION_NAME;

class BatchConsumerTest {
    @Test
    void poolAndProcessAppliesBatchAcknowledgerOutcomes() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var originatedTime = Instant.parse("2026-04-02T10:15:30Z");
        var first = CoreTestSupport.messageContainer(java.math.BigInteger.ONE, java.math.BigInteger.valueOf(11), "key-1",
                "first", Map.of("traceparent", "one"), 0, originatedTime);
        var second = CoreTestSupport.messageContainer(java.math.BigInteger.TWO, java.math.BigInteger.valueOf(12), "key-2",
                "second", Map.of(), 2, originatedTime.plusSeconds(1));
        var third = CoreTestSupport.messageContainer(java.math.BigInteger.valueOf(3), java.math.BigInteger.valueOf(13), "key-3",
                "third", Map.of(), 1, originatedTime.plusSeconds(2));
        queryService.setSelectedMessages(List.of(first, second, third));
        var seenRecords = new ArrayList<MessageRecord<String>>();
        var expectedRetryException = new IllegalStateException("retry");
        var expectedFailureException = new IllegalArgumentException("fail");
        var consumer = batchProcessor(
                queryService,
                transactionService,
                (messages, acknowledger) -> {
                    seenRecords.addAll(messages);
                    acknowledger.complete(messages.get(0));
                    acknowledger.retry(messages.get(1), Duration.ofSeconds(30), expectedRetryException);
                    acknowledger.fail(messages.get(2), expectedFailureException);
                });

        var pooled = consumer.poolAndProcess();

        assertThat(pooled).isTrue();
        assertThat(seenRecords).hasSize(3);
        assertThat(seenRecords.get(0).id()).isEqualTo(java.math.BigInteger.ONE);
        assertThat(seenRecords.get(0).message()).isEqualTo(new Message<>("key-1", originatedTime, "first",
                Map.of("traceparent", "one")));
        assertThat(seenRecords.get(1).attempt()).isEqualTo(2);
        assertThat(seenRecords.get(2).executeAfter()).isEqualTo(originatedTime.plusSeconds(2));
        assertThat(queryService.getCompletions()).hasSize(1);
        assertThat(queryService.getCompletions().getFirst().messageContainer()).isSameAs(first);
        assertThat(queryService.getRetries()).hasSize(1);
        assertThat(queryService.getRetries().getFirst().messageContainer()).isSameAs(second);
        assertThat(queryService.getRetries().getFirst().retryDuration()).isEqualTo(Duration.ofSeconds(30));
        assertThat(queryService.getRetries().getFirst().exception()).isSameAs(expectedRetryException);
        assertThat(queryService.getFailures()).hasSize(1);
        assertThat(queryService.getFailures().getFirst().messageContainer()).isSameAs(third);
        assertThat(queryService.getFailures().getFirst().exception()).isSameAs(expectedFailureException);
    }

    @Test
    void poolAndProcessRejectsBatchWithMissingOutcome() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var container = CoreTestSupport.messageContainer("payload", 0, Instant.parse("2026-04-02T10:15:30Z"));
        queryService.setSelectedMessages(List.of(container));
        var consumer = batchProcessor(
                queryService,
                transactionService,
                (messages, acknowledger) -> {
                });

        assertThatThrownBy(consumer::poolAndProcess)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Batch handler must acknowledge every message exactly once");
        assertThat(queryService.getCompletions()).isEmpty();
        assertThat(queryService.getRetries()).isEmpty();
        assertThat(queryService.getFailures()).isEmpty();
    }

    @Test
    void poolAndProcessRejectsBatchWithDuplicateOutcome() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var container = CoreTestSupport.messageContainer("payload", 0, Instant.parse("2026-04-02T10:15:30Z"));
        queryService.setSelectedMessages(List.of(container));
        var consumer = batchProcessor(
                queryService,
                transactionService,
                (messages, acknowledger) -> {
                    acknowledger.complete(messages.getFirst());
                    acknowledger.fail(messages.getFirst(), new IllegalStateException("duplicate"));
                });

        assertThatThrownBy(consumer::poolAndProcess)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Each batch record can be acknowledged only once");
        assertThat(queryService.getCompletions()).hasSize(1);
        assertThat(queryService.getRetries()).isEmpty();
        assertThat(queryService.getFailures()).isEmpty();
    }

    @Test
    void poolAndProcessAllowsAnotherOutcomeAfterFailedComplete() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var container = CoreTestSupport.messageContainer("payload", 0, Instant.parse("2026-04-02T10:15:30Z"));
        queryService.setSelectedMessages(List.of(container));
        var failure = new RetryablePersistenceException(new RuntimeException("db"), null);
        queryService.enqueueCompleteMessageResult(failure);
        var expectedFailureException = new IllegalStateException("fallback");
        var consumer = batchProcessor(
                queryService,
                transactionService,
                (messages, acknowledger) -> {
                    try {
                        acknowledger.complete(messages.getFirst());
                    } catch (RetryablePersistenceException ignored) {
                        acknowledger.fail(messages.getFirst(), expectedFailureException);
                    }
                });

        var pooled = consumer.poolAndProcess();

        assertThat(pooled).isTrue();
        assertThat(queryService.getCompletions()).isEmpty();
        assertThat(queryService.getRetries()).isEmpty();
        assertThat(queryService.getFailures()).hasSize(1);
        assertThat(queryService.getFailures().getFirst().messageContainer()).isSameAs(container);
        assertThat(queryService.getFailures().getFirst().exception()).isSameAs(expectedFailureException);
    }

    private BatchConsumer<String> batchProcessor(
            CoreTestSupport.RecordingQueryService queryService,
            CoreTestSupport.DirectTransactionService transactionService,
            BatchMessageHandler<String> batchMessageHandler
    ) {
        return new BatchConsumer<>(
                batchMessageHandler,
                QUEUE_NAME,
                SUBSCRIPTION_NAME,
                queryService,
                transactionService,
                new NoOpMessageContextPropagator(),
                new NoOpMessageConsumerTelemetry(),
                new SimpleMessageFactory(),
                ConsumerConfig.Properties.builder().build()
        );
    }
}
