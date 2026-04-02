package org.pak.messagebus.core;


import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pak.messagebus.pg.PgTableManager;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.messagebus.core.TestMessage.MESSAGE_NAME;

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

        createMessageTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1);
        tableManager.registerMessage(MESSAGE_NAME, 30);
        tableManager.registerSubscription(MESSAGE_NAME, SUBSCRIPTION_NAME_1, 30);
    }

    @AfterEach
    void tearDown() {
        clearTables();
        tableManager.stopCronJobs();
    }

    @Test
    void cleanMessagePartitionTest() {
        var now = Instant.now().truncatedTo(ChronoUnit.DAYS).minus(Duration.ofDays(10));
        pgQueryService.createMessagePartition(TestMessage.MESSAGE_NAME, now);
        pgQueryService.createMessagePartition(TestMessage.MESSAGE_NAME, now.plus(Duration.ofDays(1)));

        List<String> partitions = selectPartitions(MESSAGE_TABLE);
        assertThat(partitions).hasSize(4);
        assertPartitions(MESSAGE_TABLE, partitions);

        var tm = new PgTableManager(pgQueryService, "* * * * * ?", "* * * * * ?");
        tm.registerMessage(TestMessage.MESSAGE_NAME, 2);
        tm.cleanPartitions();

        partitions = selectPartitions(MESSAGE_TABLE);
        assertThat(partitions).hasSize(2);
        assertPartitions(MESSAGE_TABLE, partitions);

        tm.cleanPartitions();

        partitions = selectPartitions(MESSAGE_TABLE);
        assertThat(partitions).hasSize(2);
        assertPartitions(MESSAGE_TABLE, partitions);
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
        tm.registerSubscription(TestMessage.MESSAGE_NAME, SUBSCRIPTION_NAME_1, 2);
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
    void testCreateMessagePartitions() {
        tableManager.registerMessage(TestMessage.MESSAGE_NAME, 1);

        List<String> partitions = selectPartitions(MESSAGE_TABLE);

        assertThat(partitions).hasSize(2);
        assertPartitions(MESSAGE_TABLE, partitions);
    }

    @Test
    void testCreateSubscriptionPartitions() {
        tableManager.registerSubscription(TestMessage.MESSAGE_NAME, SUBSCRIPTION_NAME_1, 1);

        List<String> partitions = selectPartitions(SUBSCRIPTION_TABLE_1_HISTORY);

        assertThat(partitions).hasSize(2);

        assertPartitions(SUBSCRIPTION_TABLE_1_HISTORY, partitions);
    }

    @Test
    void testStartCronJobSuccessfully() {
        tableManager.registerSubscription(TestMessage.MESSAGE_NAME, SUBSCRIPTION_NAME_1, 1);
        tableManager.registerMessage(TestMessage.MESSAGE_NAME, 1);
        tableManager.startCronJobs();
        tableManager.stopCronJobs();
    }

    @Test
    void testStartCronJobFailed() {
        var corruptedTableManager = new PgTableManager(pgQueryService, "* * * * ?", "* * * * * ?");
        corruptedTableManager.registerSubscription(TestMessage.MESSAGE_NAME, SUBSCRIPTION_NAME_1, 1);
        corruptedTableManager.registerMessage(TestMessage.MESSAGE_NAME, 1);
        var exception = Assertions.assertThrows(RuntimeException.class, corruptedTableManager::startCronJobs);
        assertThat(exception.getMessage()).isEqualTo("CronExpression '* * * * ?' is invalid.");
    }
}
