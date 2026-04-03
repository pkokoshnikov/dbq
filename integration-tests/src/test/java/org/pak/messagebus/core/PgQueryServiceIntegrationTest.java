package org.pak.messagebus.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
    void testInsertCreatesQueuePartitionOnDemand() {
        createQueueTable();

        Instant originatedTime = Instant.now();
        var inserted = pgQueryService.insertMessage(QUEUE_NAME,
                new SimpleMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));

        assertThat(inserted).isTrue();
        assertThat(pgQueryService.getAllQueuePartitions(QUEUE_NAME)).isNotEmpty();
    }

    @Test
    void testBatchInsertCreatesQueuePartitionsOnDemand() {
        createQueueTable();

        Instant originatedTime_1 = Instant.now();
        Instant originatedTime_2 = originatedTime_1.plus(2, ChronoUnit.DAYS);
        var inserted = pgQueryService.insertBatchMessage(QUEUE_NAME,
                List.of(new SimpleMessage<>(UUID.randomUUID().toString(), originatedTime_1,
                                new TestMessage("test")),
                        new SimpleMessage<>(UUID.randomUUID().toString(), originatedTime_2,
                                new TestMessage("test"))));

        assertThat(inserted).containsExactly(true, true);
        assertThat(pgQueryService.getAllQueuePartitions(QUEUE_NAME))
                .contains(originatedTime_1.atOffset(java.time.ZoneOffset.UTC).toLocalDate(),
                        originatedTime_2.atOffset(java.time.ZoneOffset.UTC).toLocalDate());
    }

    @Test
    void testCompleteAndFailCreateHistoryPartitionOnDemand() {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1);
        Instant originatedTimeComplete = Instant.now();
        Instant originatedTimeFail = originatedTimeComplete.plus(1, ChronoUnit.DAYS);

        pgQueryService.insertBatchMessage(QUEUE_NAME,
                List.of(new SimpleMessage<>(UUID.randomUUID().toString(), originatedTimeComplete,
                                new TestMessage("complete")),
                        new SimpleMessage<>(UUID.randomUUID().toString(), originatedTimeFail,
                                new TestMessage("fail"))));

        var messages = pgQueryService.selectMessages(QUEUE_NAME, SUBSCRIPTION_NAME_1, 10);
        assertThat(messages).hasSize(2);

        var completeMessage = messages.stream()
                .filter(message -> "complete".equals(((TestMessage) message.getPayload()).getName()))
                .findFirst()
                .orElseThrow();
        var failMessage = messages.stream()
                .filter(message -> "fail".equals(((TestMessage) message.getPayload()).getName()))
                .findFirst()
                .orElseThrow();

        pgQueryService.completeMessage(SUBSCRIPTION_NAME_1, completeMessage);
        pgQueryService.failMessage(SUBSCRIPTION_NAME_1, failMessage, new RuntimeException("fail"));

        assertThat(pgQueryService.getAllHistoryPartitions(SUBSCRIPTION_NAME_1))
                .contains(originatedTimeComplete.atOffset(java.time.ZoneOffset.UTC).toLocalDate(),
                        originatedTimeFail.atOffset(java.time.ZoneOffset.UTC).toLocalDate());

        var historyMessages = selectTestMessagesFromHistory(SUBSCRIPTION_NAME_1);
        assertThat(historyMessages).hasSize(2);
        assertThat(historyMessages).anySatisfy(message -> assertThat(message.getStatus()).isEqualTo(Status.PROCESSED));
        assertThat(historyMessages).anySatisfy(message -> assertThat(message.getStatus()).isEqualTo(Status.FAILED));
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
