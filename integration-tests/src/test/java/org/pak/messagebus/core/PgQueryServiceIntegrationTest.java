package org.pak.messagebus.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pak.messagebus.core.error.MissingPartitionException;
import org.pak.messagebus.core.error.PartitionHasReferencesException;
import org.pak.messagebus.pg.PgSchemaSqlGenerator;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.messagebus.core.TestMessage.MESSAGE_NAME;

@Testcontainers
public class PgQueryServiceIntegrationTest extends BaseIntegrationTest {


    @BeforeEach
    void setUp() {
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
    void createSchemaWithSqlGeneratorTest() {
        var sqlGenerator = new PgSchemaSqlGenerator(TEST_SCHEMA);
        jdbcTemplate.execute(sqlGenerator.createMessageTable(MESSAGE_NAME));
        jdbcTemplate.execute(sqlGenerator.createSubscriptionTable(MESSAGE_NAME, SUBSCRIPTION_NAME_1));

        var originatedTime = Instant.now();
        pgQueryService.createMessagePartition(MESSAGE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);

        pgQueryService.insertMessage(MESSAGE_NAME,
                new StdMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));

        var messages = pgQueryService.selectMessages(MESSAGE_NAME, SUBSCRIPTION_NAME_1, 1);
        assertThat(messages).hasSize(1);
    }

    @Test
    void createMessagePartitionTest() {
        pgQueryService.initMessageTable(MESSAGE_NAME);
        pgQueryService.createMessagePartition(MESSAGE_NAME, Instant.now());
        var partitions = selectPartitions(MESSAGE_TABLE);

        assertThat(partitions).hasSize(1);
        assertPartitions(MESSAGE_TABLE, partitions);
    }

    @Test
    void createSubscriptionPartitionTest() {
        pgQueryService.initMessageTable(MESSAGE_NAME);
        pgQueryService.initSubscriptionTable(MESSAGE_NAME, SUBSCRIPTION_NAME_1);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, Instant.now());
        var partitions = selectPartitions(SUBSCRIPTION_TABLE_1_HISTORY);

        assertThat(partitions).hasSize(1);
        assertPartitions(SUBSCRIPTION_TABLE_1_HISTORY, partitions);
    }

    @Test
    void dropMessagePartition() {
        pgQueryService.initMessageTable(MESSAGE_NAME);
        pgQueryService.createMessagePartition(MESSAGE_NAME, Instant.now());
        var partitions = pgQueryService.getAllMessagePartitions(MESSAGE_NAME);

        pgQueryService.dropMessagePartition(MESSAGE_NAME, partitions.get(0));

        partitions = pgQueryService.getAllMessagePartitions(MESSAGE_NAME);
        assertThat(partitions).hasSize(0);
    }

    @Test
    void dropMessagePartitionHasReferencesException() {
        pgQueryService.initMessageTable(MESSAGE_NAME);
        pgQueryService.initSubscriptionTable(MESSAGE_NAME, SUBSCRIPTION_NAME_1);
        Instant originatedTime = Instant.now();
        pgQueryService.createMessagePartition(MESSAGE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);

        pgQueryService.insertMessage(MESSAGE_NAME,
                new StdMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));

        var partitions = pgQueryService.getAllMessagePartitions(MESSAGE_NAME);
        Assertions.assertThrows(PartitionHasReferencesException.class,
                () -> pgQueryService.dropMessagePartition(MESSAGE_NAME, partitions.get(0)));

        var messages = pgQueryService.selectMessages(MESSAGE_NAME, SUBSCRIPTION_NAME_1, 1);
        pgQueryService.completeMessage(SUBSCRIPTION_NAME_1, messages.get(0));

        Assertions.assertThrows(PartitionHasReferencesException.class,
                () -> pgQueryService.dropMessagePartition(MESSAGE_NAME, partitions.get(0)));

        pgQueryService.dropHistoryPartition(SUBSCRIPTION_NAME_1,
                partitions.get(0)); // history partition should be dropped first of all
        pgQueryService.dropMessagePartition(MESSAGE_NAME, partitions.get(0));
    }

    @Test
    void dropSubscriptionPartition() {
        pgQueryService.initMessageTable(MESSAGE_NAME);
        pgQueryService.initSubscriptionTable(MESSAGE_NAME, SUBSCRIPTION_NAME_1);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, Instant.now());
        var partitions = pgQueryService.getAllHistoryPartitions(SUBSCRIPTION_NAME_1);

        assertThat(partitions).hasSize(1);

        pgQueryService.dropHistoryPartition(SUBSCRIPTION_NAME_1, partitions.get(0));

        partitions = pgQueryService.getAllHistoryPartitions(SUBSCRIPTION_NAME_1);
        assertThat(partitions).hasSize(0);
    }

    @Test
    void testInsertMissingPartitionException() {
        pgQueryService.initMessageTable(MESSAGE_NAME);

        Instant originatedTime = Instant.now();
        var exception = Assertions.assertThrows(MissingPartitionException.class, () -> {
            pgQueryService.insertMessage(MESSAGE_NAME,
                    new StdMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));
        });

        assertThat(exception.getOriginationTimes().get(0)).isEqualTo(originatedTime);
    }

    @Test
    void testBatchInsertMissingPartitionException() {
        pgQueryService.initMessageTable(MESSAGE_NAME);

        Instant originatedTime_1 = Instant.now();
        Instant originatedTime_2 = Instant.now();
        var exception = Assertions.assertThrows(MissingPartitionException.class,
                () -> pgQueryService.insertBatchMessage(MESSAGE_NAME,
                        List.of(new StdMessage<>(UUID.randomUUID().toString(), originatedTime_1,
                                        new TestMessage("test")),
                                new StdMessage<>(UUID.randomUUID().toString(), originatedTime_2,
                                        new TestMessage("test")))));

        assertThat(exception.getOriginationTimes()).containsExactlyInAnyOrder(originatedTime_1, originatedTime_2);
    }

    @Test
    void testCompleteOrFailMissingPartitionException() {
        pgQueryService.initMessageTable(MESSAGE_NAME);
        pgQueryService.initSubscriptionTable(MESSAGE_NAME, SUBSCRIPTION_NAME_1);
        Instant originatedTime = Instant.now();
        pgQueryService.createMessagePartition(MESSAGE_NAME, originatedTime);

        pgQueryService.insertMessage(MESSAGE_NAME,
                new StdMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));
        var messages = pgQueryService.selectMessages(MESSAGE_NAME, SUBSCRIPTION_NAME_1, 1);

        var exception = Assertions.assertThrows(MissingPartitionException.class,
                () -> pgQueryService.completeMessage(SUBSCRIPTION_NAME_1, messages.get(0)));
        assertThat(exception.getOriginationTimes().get(0)).isEqualTo(originatedTime);

        exception = Assertions.assertThrows(MissingPartitionException.class,
                () -> pgQueryService.failMessage(SUBSCRIPTION_NAME_1, messages.get(0), new RuntimeException()));
        assertThat(exception.getOriginationTimes().get(0)).isEqualTo(originatedTime);
    }

    @Test
    void testSuccessfullySubmitMessage() {
        pgQueryService.initMessageTable(MESSAGE_NAME);
        pgQueryService.initSubscriptionTable(MESSAGE_NAME, SUBSCRIPTION_NAME_1);
        pgQueryService.initSubscriptionTable(MESSAGE_NAME, SUBSCRIPTION_NAME_2);
        Instant originatedTime = Instant.now();
        pgQueryService.createMessagePartition(MESSAGE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_2, originatedTime);

        pgQueryService.insertMessage(MESSAGE_NAME,
                new StdMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));

        var messages = pgQueryService.selectMessages(MESSAGE_NAME, SUBSCRIPTION_NAME_1, 1);
        assertThat(messages).hasSize(1);

        messages = pgQueryService.selectMessages(MESSAGE_NAME, SUBSCRIPTION_NAME_2, 1);
        assertThat(messages).hasSize(1);
    }

    @Test
    void testSuccessfullySubmitBatchMessages() {
        pgQueryService.initMessageTable(MESSAGE_NAME);
        pgQueryService.initSubscriptionTable(MESSAGE_NAME, SUBSCRIPTION_NAME_1);
        pgQueryService.initSubscriptionTable(MESSAGE_NAME, SUBSCRIPTION_NAME_2);
        Instant originatedTime = Instant.now();
        pgQueryService.createMessagePartition(MESSAGE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_2, originatedTime);

        pgQueryService.insertBatchMessage(MESSAGE_NAME,
                List.of(new StdMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")),
                        new StdMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test"))));

        var messages = pgQueryService.selectMessages(MESSAGE_NAME, SUBSCRIPTION_NAME_1, 1);
        assertThat(messages).hasSize(1);

        messages = pgQueryService.selectMessages(MESSAGE_NAME, SUBSCRIPTION_NAME_2, 1);
        assertThat(messages).hasSize(1);
    }

    @Test
    void testDuplicateKeySubmit() {
        pgQueryService.initMessageTable(MESSAGE_NAME);
        pgQueryService.initSubscriptionTable(MESSAGE_NAME, SUBSCRIPTION_NAME_1);
        Instant originatedTime = Instant.now();
        pgQueryService.createMessagePartition(MESSAGE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);

        String key = UUID.randomUUID().toString();
        pgQueryService.insertBatchMessage(MESSAGE_NAME,
                List.of(new StdMessage<>(key, originatedTime, new TestMessage("test")),
                        new StdMessage<>(key, originatedTime, new TestMessage("test"))));

        pgQueryService.insertMessage(MESSAGE_NAME,
                new StdMessage<>(key, originatedTime, new TestMessage("test")));

        var messages = pgQueryService.selectMessages(MESSAGE_NAME, SUBSCRIPTION_NAME_1, 10);
        assertThat(messages).hasSize(1);
    }

    @Test
    void testSelectMessages() {
        pgQueryService.initMessageTable(MESSAGE_NAME);
        pgQueryService.initSubscriptionTable(MESSAGE_NAME, SUBSCRIPTION_NAME_1);
        Instant originatedTime = Instant.now();
        pgQueryService.createMessagePartition(MESSAGE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);

        pgQueryService.insertBatchMessage(MESSAGE_NAME,
                List.of(new StdMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")),
                        new StdMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test"))));

        pgQueryService.insertMessage(MESSAGE_NAME,
                new StdMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));

        var messages = pgQueryService.selectMessages(MESSAGE_NAME, SUBSCRIPTION_NAME_1, 10);
        assertThat(messages).hasSize(3);
    }

    @Test
    void testCompleteMessages() {
        pgQueryService.initMessageTable(MESSAGE_NAME);
        pgQueryService.initSubscriptionTable(MESSAGE_NAME, SUBSCRIPTION_NAME_1);
        Instant originatedTime = Instant.now();
        pgQueryService.createMessagePartition(MESSAGE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);

        pgQueryService.insertBatchMessage(MESSAGE_NAME,
                List.of(new StdMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")),
                        new StdMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test"))));

        pgQueryService.insertMessage(MESSAGE_NAME,
                new StdMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));

        var messages = pgQueryService.selectMessages(MESSAGE_NAME, SUBSCRIPTION_NAME_1, 10);

        assertThat(messages).hasSize(3);
        messages.forEach(message -> {
            pgQueryService.completeMessage(SUBSCRIPTION_NAME_1, message);
        });

        messages = pgQueryService.selectMessages(MESSAGE_NAME, SUBSCRIPTION_NAME_1, 10);
        assertThat(messages).isEmpty();

        var historyMessages = selectTestMessagesFromHistory(SUBSCRIPTION_NAME_1);
        assertThat(historyMessages).hasSize(3);
        historyMessages.forEach(message -> assertThat(message.getStatus()).isEqualTo(Status.PROCESSED));
    }

    @Test
    void testFailMessages() {
        pgQueryService.initMessageTable(MESSAGE_NAME);
        pgQueryService.initSubscriptionTable(MESSAGE_NAME, SUBSCRIPTION_NAME_1);
        Instant originatedTime = Instant.now();
        pgQueryService.createMessagePartition(MESSAGE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);

        pgQueryService.insertBatchMessage(MESSAGE_NAME,
                List.of(new StdMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")),
                        new StdMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test"))));

        pgQueryService.insertMessage(MESSAGE_NAME,
                new StdMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));

        var messages = pgQueryService.selectMessages(MESSAGE_NAME, SUBSCRIPTION_NAME_1, 10);

        assertThat(messages).hasSize(3);
        messages.forEach(message -> {
            pgQueryService.failMessage(SUBSCRIPTION_NAME_1, message, new RuntimeException());
        });

        messages = pgQueryService.selectMessages(MESSAGE_NAME, SUBSCRIPTION_NAME_1, 10);
        assertThat(messages).isEmpty();

        var historyMessages = selectTestMessagesFromHistory(SUBSCRIPTION_NAME_1);
        assertThat(historyMessages).hasSize(3);
        historyMessages.forEach(message -> assertThat(message.getStatus()).isEqualTo(Status.FAILED));
    }

    @Test
    void testRetryMessages() {
        pgQueryService.initMessageTable(MESSAGE_NAME);
        pgQueryService.initSubscriptionTable(MESSAGE_NAME, SUBSCRIPTION_NAME_1);
        Instant originatedTime = Instant.now();
        pgQueryService.createMessagePartition(MESSAGE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);

        pgQueryService.insertBatchMessage(MESSAGE_NAME,
                List.of(new StdMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")),
                        new StdMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test"))));

        pgQueryService.insertMessage(MESSAGE_NAME,
                new StdMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));

        List<MessageContainer<TestMessage>> messages =
                pgQueryService.selectMessages(MESSAGE_NAME, SUBSCRIPTION_NAME_1, 10);

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
        pgQueryService.initMessageTable(MESSAGE_NAME);
        pgQueryService.initSubscriptionTable(MESSAGE_NAME, SUBSCRIPTION_NAME_1);
        Instant originatedTime = Instant.now();
        pgQueryService.createMessagePartition(MESSAGE_NAME, originatedTime);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, originatedTime);

        pgQueryService.insertMessage(MESSAGE_NAME,
                new StdMessage<>(UUID.randomUUID().toString(), originatedTime, new TestMessage("test")));

        var message = hasSize1AndGetFirst(pgQueryService.selectMessages(MESSAGE_NAME, SUBSCRIPTION_NAME_1, 1));

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
