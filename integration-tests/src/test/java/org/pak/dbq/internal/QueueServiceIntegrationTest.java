package org.pak.dbq.internal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.Message;
import org.pak.dbq.api.ProducerConfig;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.internal.persistence.Status;
import org.pak.dbq.pg.DropPartitionResult;
import org.pak.dbq.spi.ConsumerQueryService;
import org.pak.dbq.spi.ProducerQueryService;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pak.dbq.internal.TestMessage.QUEUE_NAME;

@Testcontainers
public class QueueServiceIntegrationTest extends BaseIntegrationTest {


    @BeforeEach
    void setUp() throws Exception {
        dataSource = setupDatasource();
        springTransactionService = setupSpringTransactionService(dataSource);
        jdbcTemplate = setupJdbcTemplate(dataSource);
        persistenceService = setupPersistenceService(jdbcTemplate);
        jsonbConverter = setupJsonbConverter();
        pgQueryService = setupQueryService(persistenceService, jsonbConverter);
    }

    @AfterEach
    void tearDown() {
        clearTables();
    }

    @Test
    void createQueuePartitionTest() throws Exception {
        createQueueTable();
        pgQueryService.createQueuePartition(QUEUE_NAME, Instant.now());
        var partitions = selectPartitions(QUEUE_TABLE);

        assertThat(partitions).hasSize(1);
        assertPartitions(QUEUE_TABLE, partitions);
    }

    @Test
    void createSubscriptionPartitionTest() throws Exception {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1, true);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, Instant.now());
        var partitions = selectPartitions(SUBSCRIPTION_TABLE_1_HISTORY);

        assertThat(partitions).hasSize(1);
        assertPartitions(SUBSCRIPTION_TABLE_1_HISTORY, partitions);
    }

    @Test
    void createSubscriptionTableDoesNotCreateKeyLockTableByDefault() throws Exception {
        createQueueTable();

        createSubscriptionTable(SUBSCRIPTION_NAME_1, false, false);

        assertThat(jdbcTemplate.queryForObject("SELECT to_regclass(?)", String.class,
                TEST_SCHEMA.value() + "." + SUBSCRIPTION_TABLE_1_KEY_LOCK)).isNull();
    }

    @Test
    void createSubscriptionTableCreatesKeyLockTableWhenSerializedByKeyEnabled() throws Exception {
        createQueueTable();

        createSubscriptionTable(SUBSCRIPTION_NAME_1, false, true);

        assertThat(jdbcTemplate.queryForObject("SELECT to_regclass(?)", String.class,
                TEST_SCHEMA.value() + "." + SUBSCRIPTION_TABLE_1_KEY_LOCK))
                .isEqualTo(SUBSCRIPTION_TABLE_1_KEY_LOCK);
    }

    @Test
    void selectMessagesWithKeySerializationRequiresKeyLockTable() throws Exception {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1, false, false);

        assertThatThrownBy(() -> consumerQueryService(SUBSCRIPTION_NAME_1, 1, false, true).selectMessages())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("without serializedByKey support");
    }

    @Test
    void dropQueuePartition() throws Exception {
        createQueueTable();
        pgQueryService.createQueuePartition(QUEUE_NAME, Instant.now());
        var partitions = pgQueryService.getAllQueuePartitions(QUEUE_NAME);

        pgQueryService.dropQueuePartition(QUEUE_NAME, partitions.get(0));

        partitions = pgQueryService.getAllQueuePartitions(QUEUE_NAME);
        assertThat(partitions).hasSize(0);
    }

    @Test
    void dropQueuePartitionIsIdempotent() throws Exception {
        createQueueTable();
        Instant originatedTime = Instant.now();
        pgQueryService.createQueuePartition(QUEUE_NAME, originatedTime);
        LocalDate partition = originatedTime.atOffset(java.time.ZoneOffset.UTC).toLocalDate();

        pgQueryService.dropQueuePartition(QUEUE_NAME, partition);
        pgQueryService.dropQueuePartition(QUEUE_NAME, partition);

        assertThat(pgQueryService.getAllQueuePartitions(QUEUE_NAME)).isEmpty();
    }

    @Test
    void dropQueuePartitionIgnoresAlreadyDetachedPartition() throws Exception {
        createQueueTable();
        Instant originatedTime = Instant.now();
        pgQueryService.createQueuePartition(QUEUE_NAME, originatedTime);
        LocalDate partition = originatedTime.atOffset(java.time.ZoneOffset.UTC).toLocalDate();
        String partitionName = QUEUE_TABLE + "_" + partition.format(java.time.format.DateTimeFormatter.ofPattern("yyyy_MM_dd"));

        jdbcTemplate.execute(formatter.execute("""
                ALTER TABLE ${schema}.${table} DETACH PARTITION ${schema}.${partition} CONCURRENTLY;
                """, Map.of("schema", TEST_SCHEMA.value(), "table", QUEUE_TABLE, "partition", partitionName)));

        pgQueryService.dropQueuePartition(QUEUE_NAME, partition);

        assertThat(pgQueryService.getAllQueuePartitions(QUEUE_NAME)).isEmpty();
        assertThat(jdbcTemplate.queryForObject("SELECT to_regclass(?)", String.class,
                TEST_SCHEMA.value() + "." + partitionName)).isNull();
    }

    @Test
    void dropQueuePartitionReturnsHasReferences() throws Exception {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1, true);
        Instant originatedTime = Instant.now();
        pgQueryService.createQueuePartition(QUEUE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);

        producerQueryService().insertMessage(new Message<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));

        var partitions = pgQueryService.getAllQueuePartitions(QUEUE_NAME);
        assertThat(pgQueryService.dropQueuePartition(QUEUE_NAME, partitions.get(0)))
                .isEqualTo(DropPartitionResult.HAS_REFERENCES);

        var consumerQueryService = consumerQueryService(SUBSCRIPTION_NAME_1, 1, true, false);
        var messages = consumerQueryService.selectMessages();
        consumerQueryService.completeMessage(messages.get(0));

        assertThat(pgQueryService.dropQueuePartition(QUEUE_NAME, partitions.get(0)))
                .isEqualTo(DropPartitionResult.HAS_REFERENCES);

        pgQueryService.dropHistoryPartition(SUBSCRIPTION_NAME_1,
                partitions.get(0)); // history partition should be dropped first of all
        assertThat(pgQueryService.dropQueuePartition(QUEUE_NAME, partitions.get(0)))
                .isEqualTo(DropPartitionResult.DROPPED);
    }

    @Test
    void dropSubscriptionPartition() throws Exception {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1, true);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, Instant.now());
        var partitions = pgQueryService.getAllHistoryPartitions(SUBSCRIPTION_NAME_1);

        assertThat(partitions).hasSize(1);

        pgQueryService.dropHistoryPartition(SUBSCRIPTION_NAME_1, partitions.get(0));

        partitions = pgQueryService.getAllHistoryPartitions(SUBSCRIPTION_NAME_1);
        assertThat(partitions).hasSize(0);
    }

    @Test
    void dropSubscriptionPartitionIsIdempotent() throws Exception {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1, true);
        Instant originatedTime = Instant.now();
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);
        LocalDate partition = originatedTime.atOffset(java.time.ZoneOffset.UTC).toLocalDate();

        pgQueryService.dropHistoryPartition(SUBSCRIPTION_NAME_1, partition);
        pgQueryService.dropHistoryPartition(SUBSCRIPTION_NAME_1, partition);

        assertThat(pgQueryService.getAllHistoryPartitions(SUBSCRIPTION_NAME_1)).isEmpty();
    }

    @Test
    void testInsertCreatesQueuePartitionOnDemand() throws Exception {
        createQueueTable();

        Instant originatedTime = Instant.now();
        var inserted = producerQueryService().insertMessage(
                new Message<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));

        assertThat(inserted).isTrue();
        assertThat(pgQueryService.getAllQueuePartitions(QUEUE_NAME)).isNotEmpty();
    }

    @Test
    void testInsertRoutesMessagesAcrossUtcMidnightToDifferentPartitions() throws Exception {
        createQueueTable();
        Instant beforeMidnightUtc = Instant.parse("2026-04-03T23:59:59Z");
        Instant afterMidnightUtc = Instant.parse("2026-04-04T00:00:00Z");
        String beforeKey = UUID.randomUUID().toString();
        String afterKey = UUID.randomUUID().toString();

        producerQueryService().insertBatchMessage(
                List.of(new Message<>(beforeKey, beforeMidnightUtc, new TestMessage("before")),
                        new Message<>(afterKey, afterMidnightUtc, new TestMessage("after"))));

        var rows = jdbcTemplate.query("""
                        SELECT key, tableoid::regclass::text AS partition_name
                        FROM public.test_message
                        ORDER BY key
                        """,
                (rs, rowNum) -> Map.entry(rs.getString("key"), rs.getString("partition_name")));

        assertThat(rows).containsExactlyInAnyOrder(
                Map.entry(beforeKey, "test_message_2026_04_03"),
                Map.entry(afterKey, "test_message_2026_04_04"));
    }

    @Test
    void testInsertFailsWhenExistingPartitionHasUnexpectedBounds() throws Exception {
        createQueueTable();
        Instant originatedTime = Instant.parse("2026-04-04T12:00:00Z");

        jdbcTemplate.execute("""
                CREATE TABLE public.test_message_2026_04_04
                PARTITION OF public.test_message
                FOR VALUES FROM ('2026-04-04T01:00:00Z') TO ('2026-04-05T01:00:00Z')
                """);

        assertThatThrownBy(() -> producerQueryService().insertMessage(
                new Message<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unexpected partition bounds for test_message_2026_04_04");
    }

    @Test
    void testBatchInsertCreatesQueuePartitionsOnDemand() throws Exception {
        createQueueTable();

        Instant originatedTime_1 = Instant.now();
        Instant originatedTime_2 = originatedTime_1.plus(2, ChronoUnit.DAYS);
        var inserted = producerQueryService().insertBatchMessage(
                List.of(new Message<>(UUID.randomUUID().toString(), originatedTime_1,
                                new TestMessage("test")),
                        new Message<>(UUID.randomUUID().toString(), originatedTime_2,
                                new TestMessage("test"))));

        assertThat(inserted).containsExactly(true, true);
        assertThat(pgQueryService.getAllQueuePartitions(QUEUE_NAME))
                .contains(originatedTime_1.atOffset(java.time.ZoneOffset.UTC).toLocalDate(),
                        originatedTime_2.atOffset(java.time.ZoneOffset.UTC).toLocalDate());
    }

    @Test
    void testCompleteAndFailCreateHistoryPartitionOnDemand() throws Exception {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1, true);
        Instant originatedTimeComplete = Instant.now();
        Instant originatedTimeFail = originatedTimeComplete.plus(1, ChronoUnit.DAYS);

        producerQueryService().insertBatchMessage(
                List.of(new Message<>(UUID.randomUUID().toString(), originatedTimeComplete,
                                new TestMessage("complete")),
                        new Message<>(UUID.randomUUID().toString(), originatedTimeFail,
                                new TestMessage("fail"))));

        var consumerQueryService = consumerQueryService(SUBSCRIPTION_NAME_1, 10, true, false);
        var messages = consumerQueryService.selectMessages();
        assertThat(messages).hasSize(2);

        var completeMessage = messages.stream()
                .filter(message -> "complete".equals(((TestMessage) message.getPayload()).getName()))
                .findFirst()
                .orElseThrow();
        var failMessage = messages.stream()
                .filter(message -> "fail".equals(((TestMessage) message.getPayload()).getName()))
                .findFirst()
                .orElseThrow();

        consumerQueryService.completeMessage(completeMessage);
        consumerQueryService.failMessage(failMessage, new RuntimeException("fail"));

        assertThat(pgQueryService.getAllHistoryPartitions(SUBSCRIPTION_NAME_1))
                .contains(originatedTimeComplete.atOffset(java.time.ZoneOffset.UTC).toLocalDate(),
                        originatedTimeFail.atOffset(java.time.ZoneOffset.UTC).toLocalDate());

        var historyMessages = selectTestMessagesFromHistory(SUBSCRIPTION_NAME_1);
        assertThat(historyMessages).hasSize(2);
        assertThat(historyMessages).anySatisfy(message -> assertThat(message.getStatus()).isEqualTo(Status.PROCESSED));
        assertThat(historyMessages).anySatisfy(message -> assertThat(message.getStatus()).isEqualTo(Status.FAILED));
    }

    @Test
    void testCompleteAndFailWithoutHistoryJustDeleteMessages() throws Exception {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1, false);
        Instant originatedTime = Instant.now();

        producerQueryService().insertBatchMessage(
                List.of(new Message<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("complete")),
                        new Message<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("fail"))));

        var consumerQueryService = consumerQueryService(SUBSCRIPTION_NAME_1, 10, false, false);
        var messages = consumerQueryService.selectMessages();
        assertThat(messages).hasSize(2);

        var completeMessage = messages.stream()
                .filter(message -> "complete".equals(((TestMessage) message.getPayload()).getName()))
                .findFirst()
                .orElseThrow();
        var failMessage = messages.stream()
                .filter(message -> "fail".equals(((TestMessage) message.getPayload()).getName()))
                .findFirst()
                .orElseThrow();

        consumerQueryService.completeMessage(completeMessage);
        consumerQueryService.failMessage(failMessage, new RuntimeException("fail"));

        assertThat(selectTestMessages(SUBSCRIPTION_NAME_1)).isEmpty();
        assertThat(jdbcTemplate.queryForObject("SELECT to_regclass(?)", String.class,
                TEST_SCHEMA.value() + "." + SUBSCRIPTION_TABLE_1_HISTORY)).isNull();
    }

    @Test
    void testSuccessfullySubmitMessage() throws Exception {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1, true);
        createSubscriptionTable(SUBSCRIPTION_NAME_2, true);
        Instant originatedTime = Instant.now();
        pgQueryService.createQueuePartition(QUEUE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_2, originatedTime);

        var headers = java.util.Map.of("traceparent", "00-test-parent");
        producerQueryService().insertMessage(
                new Message<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test"), headers));

        var messages = consumerQueryService(SUBSCRIPTION_NAME_1, 1, true, false).selectMessages();
        assertThat(messages).hasSize(1);
        assertThat(messages.getFirst().getHeaders()).containsEntry("traceparent", "00-test-parent");

        messages = consumerQueryService(SUBSCRIPTION_NAME_2, 1, true, false).selectMessages();
        assertThat(messages).hasSize(1);
        assertThat(messages.getFirst().getHeaders()).containsEntry("traceparent", "00-test-parent");
    }

    @Test
    void testSuccessfullySubmitBatchMessages() throws Exception {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1, true);
        createSubscriptionTable(SUBSCRIPTION_NAME_2, true);
        Instant originatedTime = Instant.now();
        pgQueryService.createQueuePartition(QUEUE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_2, originatedTime);

        producerQueryService().insertBatchMessage(
                List.of(new Message<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")),
                        new Message<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test"))));

        var messages = consumerQueryService(SUBSCRIPTION_NAME_1, 1, true, false).selectMessages();
        assertThat(messages).hasSize(1);

        messages = consumerQueryService(SUBSCRIPTION_NAME_2, 1, true, false).selectMessages();
        assertThat(messages).hasSize(1);
    }

    @Test
    void testDuplicateKeySubmit() throws Exception {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1, true);
        Instant originatedTime = Instant.now();
        pgQueryService.createQueuePartition(QUEUE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);

        String key = UUID.randomUUID().toString();
        producerQueryService().insertBatchMessage(
                List.of(new Message<>(key, originatedTime, new TestMessage("test")),
                        new Message<>(key, originatedTime, new TestMessage("test"))));

        producerQueryService().insertMessage(new Message<>(key, originatedTime, new TestMessage("test")));

        var messages = consumerQueryService(SUBSCRIPTION_NAME_1, 10, true, false).selectMessages();
        assertThat(messages).hasSize(1);
    }

    @Test
    void testSelectMessages() throws Exception {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1, true);
        Instant originatedTime = Instant.now();
        pgQueryService.createQueuePartition(QUEUE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);

        producerQueryService().insertBatchMessage(
                List.of(new Message<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")),
                        new Message<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test"))));

        producerQueryService().insertMessage(new Message<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));

        var messages = consumerQueryService(SUBSCRIPTION_NAME_1, 10, true, false).selectMessages();
        assertThat(messages).hasSize(3);
    }

    @Test
    void testCompleteMessages() throws Exception {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1, true);
        Instant originatedTime = Instant.now();
        pgQueryService.createQueuePartition(QUEUE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);

        producerQueryService().insertBatchMessage(
                List.of(new Message<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")),
                        new Message<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test"))));

        producerQueryService().insertMessage(new Message<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));

        var consumerQueryService = consumerQueryService(SUBSCRIPTION_NAME_1, 10, true, false);
        var messages = consumerQueryService.selectMessages();

        assertThat(messages).hasSize(3);
        for (var message : messages) {
            consumerQueryService.completeMessage(message);
        }

        messages = consumerQueryService.selectMessages();
        assertThat(messages).isEmpty();

        var historyMessages = selectTestMessagesFromHistory(SUBSCRIPTION_NAME_1);
        assertThat(historyMessages).hasSize(3);
        historyMessages.forEach(message -> assertThat(message.getStatus()).isEqualTo(Status.PROCESSED));
    }

    @Test
    void testFailMessages() throws Exception {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1, true);
        Instant originatedTime = Instant.now();
        pgQueryService.createQueuePartition(QUEUE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);

        producerQueryService().insertBatchMessage(
                List.of(new Message<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")),
                        new Message<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test"))));

        producerQueryService().insertMessage(new Message<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));

        var consumerQueryService = consumerQueryService(SUBSCRIPTION_NAME_1, 10, true, false);
        var messages = consumerQueryService.selectMessages();

        assertThat(messages).hasSize(3);
        for (var message : messages) {
            consumerQueryService.failMessage(message, new RuntimeException());
        }

        messages = consumerQueryService.selectMessages();
        assertThat(messages).isEmpty();

        var historyMessages = selectTestMessagesFromHistory(SUBSCRIPTION_NAME_1);
        assertThat(historyMessages).hasSize(3);
        historyMessages.forEach(message -> assertThat(message.getStatus()).isEqualTo(Status.FAILED));
    }

    @Test
    void testRetryMessages() throws Exception {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1, true);
        Instant originatedTime = Instant.now();
        pgQueryService.createQueuePartition(QUEUE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);

        producerQueryService().insertBatchMessage(
                List.of(new Message<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")),
                        new Message<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test"))));

        producerQueryService().insertMessage(new Message<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));

        var consumerQueryService = consumerQueryService(SUBSCRIPTION_NAME_1, 10, true, false);
        List<MessageContainer<TestMessage>> messages = consumerQueryService.selectMessages();

        assertThat(messages).hasSize(3);
        messages.forEach(message -> assertThat(message.getAttempt()).isEqualTo(0));

        for (var message : messages) {
            consumerQueryService.retryMessage(message, Duration.of(10, ChronoUnit.SECONDS), new RuntimeException());
        }

        messages = selectTestMessages(SUBSCRIPTION_NAME_1);
        assertThat(messages).hasSize(3);
        messages.forEach(message -> assertThat(message.getAttempt()).isEqualTo(1));
    }

    @Test
    void testRetryMessagesUsesLatestRetryDuration() throws Exception {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1, true);
        Instant originatedTime = Instant.now();
        pgQueryService.createQueuePartition(QUEUE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);

        producerQueryService().insertMessage(new Message<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));

        var consumerQueryService = consumerQueryService(SUBSCRIPTION_NAME_1, 1, true, false);
        var message = hasSize1AndGetFirst(consumerQueryService.selectMessages());

        consumerQueryService.retryMessage(message,
                Duration.of(5, ChronoUnit.SECONDS),
                new RuntimeException(TEST_EXCEPTION_MESSAGE));

        var firstRetryMessage = hasSize1AndGetFirst(selectTestMessages(SUBSCRIPTION_NAME_1));
        assertThat(firstRetryMessage.getAttempt()).isEqualTo(1);

        consumerQueryService.retryMessage(firstRetryMessage,
                Duration.of(2, ChronoUnit.MINUTES),
                new RuntimeException(TEST_EXCEPTION_MESSAGE));

        var secondRetryMessage = hasSize1AndGetFirst(selectTestMessages(SUBSCRIPTION_NAME_1));
        assertThat(secondRetryMessage.getAttempt()).isEqualTo(2);
        assertThat(secondRetryMessage.getExecuteAfter())
                .isAfter(firstRetryMessage.getExecuteAfter().plus(Duration.of(90, ChronoUnit.SECONDS)));
    }

    @Test
    void testSelectMessagesWithoutKeySerializationAllowsConcurrentPollingForSameKey() throws Exception {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1, true);
        Instant originatedTime = Instant.now();
        String key = UUID.randomUUID().toString();

        producerQueryService().insertBatchMessage(
                List.of(
                        new Message<>(key, originatedTime, new TestMessage("first")),
                        new Message<>(key, originatedTime.plusMillis(1), new TestMessage("second"))));

        var executor = Executors.newFixedThreadPool(2);
        var firstLocked = new CountDownLatch(1);
        var releaseFirst = new CountDownLatch(1);

        try {
            var firstFuture = executor.submit(() -> springTransactionService.inTransaction(() -> {
                var messages = selectMessagesUnchecked(SUBSCRIPTION_NAME_1, false);
                firstLocked.countDown();
                awaitLatch(releaseFirst);
                return messages;
            }));

            assertThat(firstLocked.await(5, TimeUnit.SECONDS)).isTrue();

            var secondFuture = executor.submit(() ->
                    springTransactionService.inTransaction(() ->
                            selectMessagesUnchecked(SUBSCRIPTION_NAME_1, false)));

            var secondMessages = secondFuture.get(5, TimeUnit.SECONDS);
            assertThat(secondMessages).hasSize(1);

            releaseFirst.countDown();
            var firstMessages = firstFuture.get(5, TimeUnit.SECONDS);
            assertThat(firstMessages).hasSize(1);
        } finally {
            releaseFirst.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void testSelectMessagesWithKeySerializationSkipsConcurrentPollingForSameKey() throws Exception {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1, true, true);
        Instant originatedTime = Instant.now();
        String key = UUID.randomUUID().toString();

        producerQueryService().insertBatchMessage(
                List.of(
                        new Message<>(key, originatedTime, new TestMessage("first")),
                        new Message<>(key, originatedTime.plusMillis(1), new TestMessage("second"))));

        var executor = Executors.newFixedThreadPool(2);
        var firstLocked = new CountDownLatch(1);
        var releaseFirst = new CountDownLatch(1);

        try {
            var firstFuture = executor.submit(() -> springTransactionService.inTransaction(() -> {
                var messages = selectMessagesUnchecked(SUBSCRIPTION_NAME_1, true);
                firstLocked.countDown();
                awaitLatch(releaseFirst);
                return messages;
            }));

            assertThat(firstLocked.await(5, TimeUnit.SECONDS)).isTrue();

            var secondFuture = executor.submit(() ->
                    springTransactionService.inTransaction(() ->
                            selectMessagesUnchecked(SUBSCRIPTION_NAME_1, true)));

            var secondMessages = secondFuture.get(5, TimeUnit.SECONDS);
            assertThat(secondMessages).isEmpty();

            releaseFirst.countDown();
            var firstMessages = firstFuture.get(5, TimeUnit.SECONDS);
            assertThat(firstMessages).hasSize(1);
        } finally {
            releaseFirst.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void testSelectMessagesWithKeySerializationStillAllowsConcurrentPollingForDifferentKeys() throws Exception {
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1, true, true);
        Instant originatedTime = Instant.now();

        producerQueryService().insertBatchMessage(
                List.of(
                        new Message<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("first")),
                        new Message<>(UUID.randomUUID().toString(), originatedTime.plusMillis(1), new TestMessage("second"))));

        var executor = Executors.newFixedThreadPool(2);
        var firstLocked = new CountDownLatch(1);
        var releaseFirst = new CountDownLatch(1);

        try {
            var firstFuture = executor.submit(() -> springTransactionService.inTransaction(() -> {
                var messages = selectMessagesUnchecked(SUBSCRIPTION_NAME_1, true);
                firstLocked.countDown();
                awaitLatch(releaseFirst);
                return messages;
            }));

            assertThat(firstLocked.await(5, TimeUnit.SECONDS)).isTrue();

            var secondFuture = executor.submit(() ->
                    springTransactionService.inTransaction(() ->
                            selectMessagesUnchecked(SUBSCRIPTION_NAME_1, true)));

            var secondMessages = secondFuture.get(5, TimeUnit.SECONDS);
            assertThat(secondMessages).hasSize(1);

            releaseFirst.countDown();
            var firstMessages = firstFuture.get(5, TimeUnit.SECONDS);
            assertThat(firstMessages).hasSize(1);
            assertThat(firstMessages.getFirst().getKey()).isNotEqualTo(secondMessages.getFirst().getKey());
        } finally {
            releaseFirst.countDown();
            executor.shutdownNow();
        }
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private List<MessageContainer<TestMessage>> selectMessagesUnchecked(
            SubscriptionId subscriptionId,
            boolean serializedByKey
    ) {
        try {
            return consumerQueryService(subscriptionId, 1, true, serializedByKey).selectMessages();
        } catch (Exception e) {
            return sneakyThrow(e);
        }
    }

    private ProducerQueryService producerQueryService() {
        return pgQueryService.createProducerQueryService(ProducerConfig.<TestMessage>builder()
                .queueName(QUEUE_NAME)
                .clazz(TestMessage.class)
                .build());
    }

    private ConsumerQueryService consumerQueryService(
            SubscriptionId subscriptionId,
            int maxPollRecords,
            boolean historyEnabled,
            boolean serializedByKey
    ) {
        return pgQueryService.createConsumerQueryService(ConsumerConfig.<TestMessage>builder()
                .queueName(QUEUE_NAME)
                .subscriptionId(subscriptionId)
                .messageHandler(message -> {
                })
                .properties(ConsumerConfig.Properties.builder()
                        .maxPollRecords(maxPollRecords)
                        .historyEnabled(historyEnabled)
                        .serializedByKey(serializedByKey)
                        .build())
                .build());
    }

    @SuppressWarnings("unchecked")
    private static <T, E extends Throwable> T sneakyThrow(Throwable throwable) throws E {
        throw (E) throwable;
    }
}
