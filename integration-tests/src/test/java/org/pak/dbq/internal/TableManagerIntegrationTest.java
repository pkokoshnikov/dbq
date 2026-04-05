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
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.dbq.internal.TestMessage.QUEUE_NAME;

@Testcontainers
@Slf4j
class TableManagerIntegrationTest extends BaseIntegrationTest {

    @BeforeEach
    void setUp() {
        dataSource = setupDatasource();
        springTransactionService = setupSpringTransactionService(dataSource);
        jdbcTemplate = setupJdbcTemplate(dataSource);
        persistenceService = setupPersistenceService(jdbcTemplate);
        jsonbConverter = setupJsonbConverter();
        schemaSqlGenerator = setupSchemaSqlGenerator();
        pgQueryService = setupQueryService(persistenceService, jsonbConverter);
        tableManager = setupTableManager(pgQueryService);

        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1, true);
        tableManager.registerQueue(QUEUE_NAME, 30);
        tableManager.registerSubscription(QUEUE_NAME, SUBSCRIPTION_NAME_1, 30, true);
    }

    @AfterEach
    void tearDown() {
        clearTables();
        tableManager.stopCronJobs();
    }

    @Test
    void cleanMessagePartitionTest() {
        var now = Instant.now().truncatedTo(ChronoUnit.DAYS).minus(Duration.ofDays(10));
        pgQueryService.createQueuePartition(TestMessage.QUEUE_NAME, now);
        pgQueryService.createQueuePartition(TestMessage.QUEUE_NAME, now.plus(Duration.ofDays(1)));

        List<String> partitions = selectPartitions(QUEUE_TABLE);
        assertThat(partitions).hasSize(4);
        assertPartitions(QUEUE_TABLE, partitions);

        var tm = new PgTableManager(pgQueryService, "* * * * * ?", "* * * * * ?");
        tm.registerQueue(TestMessage.QUEUE_NAME, 2);
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
    void cleanSubscriptionPartitionTest() {
        var now = Instant.now().truncatedTo(ChronoUnit.DAYS).minus(Duration.ofDays(10));
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, now);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, now.plus(Duration.ofDays(1)));

        List<String> partitions = selectPartitions(SUBSCRIPTION_TABLE_1_HISTORY);
        assertThat(partitions).hasSize(4);
        assertPartitions(SUBSCRIPTION_TABLE_1_HISTORY, partitions);

        var tm = new PgTableManager(pgQueryService, "* * * * * ?", "* * * * * ?");
        tm.registerSubscription(TestMessage.QUEUE_NAME, SUBSCRIPTION_NAME_1, 2, true);
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
    void cleanPartitionsUsesUtcDateForRetentionCutoff() {
        var fixedClock = Clock.fixed(Instant.parse("2026-04-04T21:30:00Z"), ZoneId.of("Europe/Moscow"));
        var tm = new PgTableManager(pgQueryService, "* * * * * ?", "* * * * * ?", fixedClock);

        pgQueryService.createQueuePartition(QUEUE_NAME, Instant.parse("2026-04-02T12:00:00Z"));
        pgQueryService.createQueuePartition(QUEUE_NAME, Instant.parse("2026-04-03T12:00:00Z"));

        tm.registerQueue(QUEUE_NAME, 1);
        tm.cleanPartitions();

        var partitions = selectPartitions(QUEUE_TABLE);
        assertThat(partitions).doesNotContain("test_message_2026_04_02");
        assertThat(partitions).contains("test_message_2026_04_03");
    }

    @Test
    void testCreateMessagePartitions() {
        tableManager.registerQueue(TestMessage.QUEUE_NAME, 1);

        List<String> partitions = selectPartitions(QUEUE_TABLE);

        assertThat(partitions).hasSize(2);
        assertPartitions(QUEUE_TABLE, partitions);
    }

    @Test
    void testCreateSubscriptionPartitions() {
        tableManager.registerSubscription(TestMessage.QUEUE_NAME, SUBSCRIPTION_NAME_1, 1, true);

        List<String> partitions = selectPartitions(SUBSCRIPTION_TABLE_1_HISTORY);

        assertThat(partitions).hasSize(2);

        assertPartitions(SUBSCRIPTION_TABLE_1_HISTORY, partitions);
    }

    @Test
    void testStartCronJobSuccessfully() {
        tableManager.registerSubscription(TestMessage.QUEUE_NAME, SUBSCRIPTION_NAME_1, 1, true);
        tableManager.registerQueue(TestMessage.QUEUE_NAME, 1);
        tableManager.startCronJobs();
        tableManager.stopCronJobs();
    }

    @Test
    void testStartCronJobFailed() {
        var corruptedTableManager = new PgTableManager(pgQueryService, "* * * * ?", "* * * * * ?");
        corruptedTableManager.registerSubscription(TestMessage.QUEUE_NAME, SUBSCRIPTION_NAME_1, 1, true);
        corruptedTableManager.registerQueue(TestMessage.QUEUE_NAME, 1);
        var exception = Assertions.assertThrows(RuntimeException.class, corruptedTableManager::startCronJobs);
        assertThat(exception.getMessage()).isEqualTo("CronExpression '* * * * ?' is invalid.");
    }
}
