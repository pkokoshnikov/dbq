package org.pak.dbq.internal;


import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pak.dbq.pg.PgTableManager;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.dbq.internal.TestMessage.QUEUE_NAME;

@Testcontainers
@Slf4j
class TableManagerIntegrationTest extends BaseIntegrationTest {

    @BeforeEach
    void setUp() throws Exception {
        dataSource = setupDatasource();
        springTransactionService = setupSpringTransactionService(dataSource);
        jdbcTemplate = setupJdbcTemplate(dataSource);
        persistenceService = setupPersistenceService(jdbcTemplate);
        jsonbConverter = setupJsonbConverter();
        pgQueryService = setupQueryService(persistenceService, jsonbConverter);
        tableManager = setupTableManager(persistenceService);

        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1, true);
        tableManager.registerQueue(QUEUE_NAME, 30, false);
        tableManager.registerSubscription(QUEUE_NAME, SUBSCRIPTION_NAME_1, true, false);
    }

    @AfterEach
    void tearDown() {
        clearTables();
        tableManager.stopCronJobs();
    }

    @Test
    void cleanMessagePartitionTest() throws Exception {
        var now = Instant.now().truncatedTo(ChronoUnit.DAYS).minus(Duration.ofDays(10));
        partitionService().createQueuePartition(TestMessage.QUEUE_NAME, now);
        partitionService().createQueuePartition(TestMessage.QUEUE_NAME, now.plus(Duration.ofDays(1)));

        List<String> partitions = selectPartitions(QUEUE_TABLE);
        assertThat(partitions).hasSize(4);
        assertPartitions(QUEUE_TABLE, partitions);

        var tm = new PgTableManager(
                persistenceService,
                TEST_SCHEMA,
                new org.pak.dbq.pg.QueuePartitionService(TEST_SCHEMA, persistenceService),
                "* * * * * ?",
                "* * * * * ?");
        tm.registerQueue(TestMessage.QUEUE_NAME, 2, false);
        tm.cleanPartitions();

        partitions = selectPartitions(QUEUE_TABLE);
        assertThat(partitions).hasSize(2);
        assertPartitions(QUEUE_TABLE, partitions);

        tm.cleanPartitions();

        partitions = selectPartitions(QUEUE_TABLE);
        assertThat(partitions).hasSize(2);
        assertPartitions(QUEUE_TABLE, partitions);
    }

    @Test
    void cleanSubscriptionPartitionTest() throws Exception {
        var now = Instant.now().truncatedTo(ChronoUnit.DAYS).minus(Duration.ofDays(10));
        partitionService().createHistoryPartition(SUBSCRIPTION_NAME_1, now);
        partitionService().createHistoryPartition(SUBSCRIPTION_NAME_1, now.plus(Duration.ofDays(1)));

        List<String> partitions = selectPartitions(SUBSCRIPTION_TABLE_1_HISTORY);
        assertThat(partitions).hasSize(4);
        assertPartitions(SUBSCRIPTION_TABLE_1_HISTORY, partitions);

        var tm = new PgTableManager(
                persistenceService,
                TEST_SCHEMA,
                new org.pak.dbq.pg.QueuePartitionService(TEST_SCHEMA, persistenceService),
                "* * * * * ?",
                "* * * * * ?");
        tm.registerQueue(TestMessage.QUEUE_NAME, 2, false);
        tm.registerSubscription(TestMessage.QUEUE_NAME, SUBSCRIPTION_NAME_1, true, false);
        tm.cleanPartitions();

        partitions = selectPartitions(SUBSCRIPTION_TABLE_1_HISTORY);
        assertThat(partitions).hasSize(2);
        assertPartitions(SUBSCRIPTION_TABLE_1_HISTORY, partitions);

        tm.cleanPartitions();

        partitions = selectPartitions(SUBSCRIPTION_TABLE_1_HISTORY);
        assertThat(partitions).hasSize(2);
        assertPartitions(SUBSCRIPTION_TABLE_1_HISTORY, partitions);
    }

    @Test
    void cleanPartitionsUsesUtcDateForRetentionCutoff() throws Exception {
        var fixedClock = Clock.fixed(Instant.parse("2026-04-04T21:30:00Z"), ZoneId.of("Europe/Moscow"));
        var tm = new PgTableManager(
                persistenceService,
                TEST_SCHEMA,
                new org.pak.dbq.pg.QueuePartitionService(TEST_SCHEMA, persistenceService),
                "* * * * * ?",
                "* * * * * ?",
                fixedClock);

        partitionService().createQueuePartition(QUEUE_NAME, Instant.parse("2026-04-02T12:00:00Z"));
        partitionService().createQueuePartition(QUEUE_NAME, Instant.parse("2026-04-03T12:00:00Z"));

        tm.registerQueue(QUEUE_NAME, 1, false);
        tm.cleanPartitions();

        var partitions = selectPartitions(QUEUE_TABLE);
        assertThat(partitions).doesNotContain("test_message_2026_04_02");
        assertThat(partitions).contains("test_message_2026_04_03");
    }

    @Test
    void testCreateMessagePartitions() throws Exception {
        tableManager.registerQueue(TestMessage.QUEUE_NAME, 30, false);

        List<String> partitions = selectPartitions(QUEUE_TABLE);

        assertThat(partitions).hasSize(2);
        assertPartitions(QUEUE_TABLE, partitions);
    }

    @Test
    void testCreateSubscriptionPartitions() throws Exception {
        tableManager.registerSubscription(TestMessage.QUEUE_NAME, SUBSCRIPTION_NAME_1, true, false);

        List<String> partitions = selectPartitions(SUBSCRIPTION_TABLE_1_HISTORY);

        assertThat(partitions).hasSize(2);

        assertPartitions(SUBSCRIPTION_TABLE_1_HISTORY, partitions);
    }

    @Test
    void testAutoDdlDoesNotFailWhenTablesAlreadyExist() throws Exception {
        var tm = new PgTableManager(
                persistenceService,
                TEST_SCHEMA,
                new org.pak.dbq.pg.QueuePartitionService(TEST_SCHEMA, persistenceService),
                "* * * * * ?",
                "* * * * * ?");

        Assertions.assertDoesNotThrow(() -> {
            tm.registerQueue(TestMessage.QUEUE_NAME, 30, true);
            tm.registerSubscription(TestMessage.QUEUE_NAME, SUBSCRIPTION_NAME_1, true, false);
        });

        assertThat(selectPartitions(QUEUE_TABLE)).hasSize(2);
        assertThat(selectPartitions(SUBSCRIPTION_TABLE_1_HISTORY)).hasSize(2);
    }

    @Test
    void testStartCronJobSuccessfully() throws Exception {
        tableManager.registerQueue(TestMessage.QUEUE_NAME, 30, false);
        tableManager.registerSubscription(TestMessage.QUEUE_NAME, SUBSCRIPTION_NAME_1, true, false);
        tableManager.startCronJobs();
        tableManager.stopCronJobs();
    }

    @Test
    void testRejectConflictingRetentionRegistration() throws Exception {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> tableManager.registerQueue(TestMessage.QUEUE_NAME, 1, false));
        Assertions.assertThrows(IllegalStateException.class,
                () -> new PgTableManager(
                        persistenceService,
                        TEST_SCHEMA,
                        new org.pak.dbq.pg.QueuePartitionService(TEST_SCHEMA, persistenceService),
                        "* * * * * ?",
                        "* * * * * ?")
                        .registerSubscription(TestMessage.QUEUE_NAME, SUBSCRIPTION_NAME_1, true, false));
    }

    @Test
    void testStartCronJobFailed() throws Exception {
        var corruptedTableManager = new PgTableManager(
                persistenceService,
                TEST_SCHEMA,
                new org.pak.dbq.pg.QueuePartitionService(TEST_SCHEMA, persistenceService),
                "* * * * ?",
                "* * * * * ?");
        corruptedTableManager.registerQueue(TestMessage.QUEUE_NAME, 1, false);
        corruptedTableManager.registerSubscription(TestMessage.QUEUE_NAME, SUBSCRIPTION_NAME_1, true, false);
        var exception = Assertions.assertThrows(RuntimeException.class, corruptedTableManager::startCronJobs);
        assertThat(exception.getMessage()).isEqualTo("CronExpression '* * * * ?' is invalid.");
    }
}
