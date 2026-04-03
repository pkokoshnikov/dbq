package org.pak.messagebus.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pak.messagebus.core.error.MissingPartitionException;
import org.pak.messagebus.core.error.PartitionHasReferencesException;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.messagebus.core.TestMessage.QUEUE_NAME;

@Testcontainers
public class PgQueryServiceIntegrationTest extends BaseIntegrationTest {


    @BeforeEach
    void setUp() {
        dataSource = setupDatasource();
        springTransactionService = setupSpringTransactionService(dataSource);
        jdbcTemplate = setupJdbcTemplate(dataSource);
        persistenceService = setupPersistenceService(jdbcTemplate);
        jsonbConverter = setupJsonbConverter();
        schemaSqlGenerator = setupSchemaSqlGenerator();
        pgQueryService = setupQueryService(persistenceService, jsonbConverter);
    }

    @AfterEach
    void tearDown() {
        clearTables();
    }

    @Test
    void createQueuePartitionTest() {
        createQueueTable();
        pgQueryService.createQueuePartition(QUEUE_NAME, Instant.now());
        var partitions = selectPartitions(QUEUE_TABLE);

        assertThat(partitions).hasSize(1);
        assertPartitions(QUEUE_TABLE, partitions);
    }

    @Test
    void createSubscriptionPartitionTest() {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, Instant.now());
        var partitions = selectPartitions(SUBSCRIPTION_TABLE_1_HISTORY);

        assertThat(partitions).hasSize(1);
        assertPartitions(SUBSCRIPTION_TABLE_1_HISTORY, partitions);
    }

    @Test
    void dropQueuePartition() {
        createQueueTable();
        pgQueryService.createQueuePartition(QUEUE_NAME, Instant.now());
        var partitions = pgQueryService.getAllQueuePartitions(QUEUE_NAME);

        pgQueryService.dropQueuePartition(QUEUE_NAME, partitions.get(0));

        partitions = pgQueryService.getAllQueuePartitions(QUEUE_NAME);
        assertThat(partitions).hasSize(0);
    }

    @Test
    void dropQueuePartitionHasReferencesException() {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1);
        Instant originatedTime = Instant.now();
        pgQueryService.createQueuePartition(QUEUE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);

        pgQueryService.insertMessage(QUEUE_NAME,
                new SimpleMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));

        var partitions = pgQueryService.getAllQueuePartitions(QUEUE_NAME);
        Assertions.assertThrows(PartitionHasReferencesException.class,
                () -> pgQueryService.dropQueuePartition(QUEUE_NAME, partitions.get(0)));

        var messages = pgQueryService.selectMessages(QUEUE_NAME, SUBSCRIPTION_NAME_1, 1);
        pgQueryService.completeMessage(SUBSCRIPTION_NAME_1, messages.get(0));

        Assertions.assertThrows(PartitionHasReferencesException.class,
                () -> pgQueryService.dropQueuePartition(QUEUE_NAME, partitions.get(0)));

        pgQueryService.dropHistoryPartition(SUBSCRIPTION_NAME_1,
                partitions.get(0)); // history partition should be dropped first of all
        pgQueryService.dropQueuePartition(QUEUE_NAME, partitions.get(0));
    }

    @Test
    void dropSubscriptionPartition() {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, Instant.now());
        var partitions = pgQueryService.getAllHistoryPartitions(SUBSCRIPTION_NAME_1);

        assertThat(partitions).hasSize(1);

        pgQueryService.dropHistoryPartition(SUBSCRIPTION_NAME_1, partitions.get(0));

        partitions = pgQueryService.getAllHistoryPartitions(SUBSCRIPTION_NAME_1);
        assertThat(partitions).hasSize(0);
    }

    @Test
    void testInsertMissingPartitionException() {
        createQueueTable();

        Instant originatedTime = Instant.now();
        var exception = Assertions.assertThrows(MissingPartitionException.class, () -> {
            pgQueryService.insertMessage(QUEUE_NAME,
                    new SimpleMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));
        });

        assertThat(exception.getOriginationTimes().get(0)).isEqualTo(originatedTime);
    }

    @Test
    void testBatchInsertMissingPartitionException() {
        createQueueTable();

        Instant originatedTime_1 = Instant.now();
        Instant originatedTime_2 = Instant.now();
        var exception = Assertions.assertThrows(MissingPartitionException.class,
                () -> pgQueryService.insertBatchMessage(QUEUE_NAME,
                        List.of(new SimpleMessage<>(UUID.randomUUID().toString(), originatedTime_1,
                                        new TestMessage("test")),
                                new SimpleMessage<>(UUID.randomUUID().toString(), originatedTime_2,
                                        new TestMessage("test")))));

        assertThat(exception.getOriginationTimes()).containsExactlyInAnyOrder(originatedTime_1, originatedTime_2);
    }

    @Test
    void testCompleteOrFailMissingPartitionException() {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1);
        Instant originatedTime = Instant.now();
        pgQueryService.createQueuePartition(QUEUE_NAME, originatedTime);

        pgQueryService.insertMessage(QUEUE_NAME,
                new SimpleMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));
        var messages = pgQueryService.selectMessages(QUEUE_NAME, SUBSCRIPTION_NAME_1, 1);

        var exception = Assertions.assertThrows(MissingPartitionException.class,
                () -> pgQueryService.completeMessage(SUBSCRIPTION_NAME_1, messages.get(0)));
        assertThat(exception.getOriginationTimes().get(0)).isEqualTo(originatedTime);

        exception = Assertions.assertThrows(MissingPartitionException.class,
                () -> pgQueryService.failMessage(SUBSCRIPTION_NAME_1, messages.get(0), new RuntimeException()));
        assertThat(exception.getOriginationTimes().get(0)).isEqualTo(originatedTime);
    }

    @Test
    void testSuccessfullySubmitMessage() {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1);
        createSubscriptionTable(SUBSCRIPTION_NAME_2);
        Instant originatedTime = Instant.now();
        pgQueryService.createQueuePartition(QUEUE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_2, originatedTime);

        var headers = java.util.Map.of("traceparent", "00-test-parent");
        pgQueryService.insertMessage(QUEUE_NAME,
                new SimpleMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test"), headers));

        var messages = pgQueryService.selectMessages(QUEUE_NAME, SUBSCRIPTION_NAME_1, 1);
        assertThat(messages).hasSize(1);
        assertThat(messages.getFirst().getHeaders()).containsEntry("traceparent", "00-test-parent");

        messages = pgQueryService.selectMessages(QUEUE_NAME, SUBSCRIPTION_NAME_2, 1);
        assertThat(messages).hasSize(1);
        assertThat(messages.getFirst().getHeaders()).containsEntry("traceparent", "00-test-parent");
    }

    @Test
    void testSuccessfullySubmitBatchMessages() {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1);
        createSubscriptionTable(SUBSCRIPTION_NAME_2);
        Instant originatedTime = Instant.now();
        pgQueryService.createQueuePartition(QUEUE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_2, originatedTime);

        pgQueryService.insertBatchMessage(QUEUE_NAME,
                List.of(new SimpleMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")),
                        new SimpleMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test"))));

        var messages = pgQueryService.selectMessages(QUEUE_NAME, SUBSCRIPTION_NAME_1, 1);
        assertThat(messages).hasSize(1);

        messages = pgQueryService.selectMessages(QUEUE_NAME, SUBSCRIPTION_NAME_2, 1);
        assertThat(messages).hasSize(1);
    }

    @Test
    void testDuplicateKeySubmit() {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1);
        Instant originatedTime = Instant.now();
        pgQueryService.createQueuePartition(QUEUE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);

        String key = UUID.randomUUID().toString();
        pgQueryService.insertBatchMessage(QUEUE_NAME,
                List.of(new SimpleMessage<>(key, originatedTime, new TestMessage("test")),
                        new SimpleMessage<>(key, originatedTime, new TestMessage("test"))));

        pgQueryService.insertMessage(QUEUE_NAME,
                new SimpleMessage<>(key, originatedTime, new TestMessage("test")));

        var messages = pgQueryService.selectMessages(QUEUE_NAME, SUBSCRIPTION_NAME_1, 10);
        assertThat(messages).hasSize(1);
    }

    @Test
    void testSelectMessages() {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1);
        Instant originatedTime = Instant.now();
        pgQueryService.createQueuePartition(QUEUE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);

        pgQueryService.insertBatchMessage(QUEUE_NAME,
                List.of(new SimpleMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")),
                        new SimpleMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test"))));

        pgQueryService.insertMessage(QUEUE_NAME,
                new SimpleMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));

        var messages = pgQueryService.selectMessages(QUEUE_NAME, SUBSCRIPTION_NAME_1, 10);
        assertThat(messages).hasSize(3);
    }

    @Test
    void testCompleteMessages() {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1);
        Instant originatedTime = Instant.now();
        pgQueryService.createQueuePartition(QUEUE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);

        pgQueryService.insertBatchMessage(QUEUE_NAME,
                List.of(new SimpleMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")),
                        new SimpleMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test"))));

        pgQueryService.insertMessage(QUEUE_NAME,
                new SimpleMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));

        var messages = pgQueryService.selectMessages(QUEUE_NAME, SUBSCRIPTION_NAME_1, 10);

        assertThat(messages).hasSize(3);
        messages.forEach(message -> {
            pgQueryService.completeMessage(SUBSCRIPTION_NAME_1, message);
        });

        messages = pgQueryService.selectMessages(QUEUE_NAME, SUBSCRIPTION_NAME_1, 10);
        assertThat(messages).isEmpty();

        var historyMessages = selectTestMessagesFromHistory(SUBSCRIPTION_NAME_1);
        assertThat(historyMessages).hasSize(3);
        historyMessages.forEach(message -> assertThat(message.getStatus()).isEqualTo(Status.PROCESSED));
    }

    @Test
    void testFailMessages() {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1);
        Instant originatedTime = Instant.now();
        pgQueryService.createQueuePartition(QUEUE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);

        pgQueryService.insertBatchMessage(QUEUE_NAME,
                List.of(new SimpleMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")),
                        new SimpleMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test"))));

        pgQueryService.insertMessage(QUEUE_NAME,
                new SimpleMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));

        var messages = pgQueryService.selectMessages(QUEUE_NAME, SUBSCRIPTION_NAME_1, 10);

        assertThat(messages).hasSize(3);
        messages.forEach(message -> {
            pgQueryService.failMessage(SUBSCRIPTION_NAME_1, message, new RuntimeException());
        });

        messages = pgQueryService.selectMessages(QUEUE_NAME, SUBSCRIPTION_NAME_1, 10);
        assertThat(messages).isEmpty();

        var historyMessages = selectTestMessagesFromHistory(SUBSCRIPTION_NAME_1);
        assertThat(historyMessages).hasSize(3);
        historyMessages.forEach(message -> assertThat(message.getStatus()).isEqualTo(Status.FAILED));
    }

    @Test
    void testRetryMessages() {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1);
        Instant originatedTime = Instant.now();
        pgQueryService.createQueuePartition(QUEUE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);

        pgQueryService.insertBatchMessage(QUEUE_NAME,
                List.of(new SimpleMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")),
                        new SimpleMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test"))));

        pgQueryService.insertMessage(QUEUE_NAME,
                new SimpleMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));

        List<MessageContainer<TestMessage>> messages =
                pgQueryService.selectMessages(QUEUE_NAME, SUBSCRIPTION_NAME_1, 10);

        assertThat(messages).hasSize(3);
        messages.forEach(message -> assertThat(message.getAttempt()).isEqualTo(0));

        messages.forEach(message -> pgQueryService.retryMessage(SUBSCRIPTION_NAME_1, message,
                Duration.of(10, ChronoUnit.SECONDS),
                new RuntimeException()));

        messages = selectTestMessages(SUBSCRIPTION_NAME_1);
        assertThat(messages).hasSize(3);
        messages.forEach(message -> assertThat(message.getAttempt()).isEqualTo(1));
    }

    @Test
    void testRetryMessagesUsesLatestRetryDuration() {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1);
        Instant originatedTime = Instant.now();
        pgQueryService.createQueuePartition(QUEUE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);

        pgQueryService.insertMessage(QUEUE_NAME,
                new SimpleMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));

        var message = hasSize1AndGetFirst(pgQueryService.selectMessages(QUEUE_NAME, SUBSCRIPTION_NAME_1, 1));

        pgQueryService.retryMessage(SUBSCRIPTION_NAME_1, message,
                Duration.of(5, ChronoUnit.SECONDS),
                new RuntimeException(TEST_EXCEPTION_MESSAGE));

        var firstRetryMessage = hasSize1AndGetFirst(selectTestMessages(SUBSCRIPTION_NAME_1));
        assertThat(firstRetryMessage.getAttempt()).isEqualTo(1);

        pgQueryService.retryMessage(SUBSCRIPTION_NAME_1, firstRetryMessage,
                Duration.of(2, ChronoUnit.MINUTES),
                new RuntimeException(TEST_EXCEPTION_MESSAGE));

        var secondRetryMessage = hasSize1AndGetFirst(selectTestMessages(SUBSCRIPTION_NAME_1));
        assertThat(secondRetryMessage.getAttempt()).isEqualTo(2);
        assertThat(secondRetryMessage.getExecuteAfter())
                .isAfter(firstRetryMessage.getExecuteAfter().plus(Duration.of(90, ChronoUnit.SECONDS)));
    }
}
