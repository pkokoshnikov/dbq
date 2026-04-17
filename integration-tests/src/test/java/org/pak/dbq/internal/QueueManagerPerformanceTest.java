package org.pak.dbq.internal;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.ProducerConfig;
import org.pak.dbq.api.QueueConfig;
import org.pak.dbq.api.QueueManager;
import org.pak.dbq.internal.support.SimpleMessageFactory;
import org.pak.dbq.pg.PgQueryService;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.vibur.dbcp.ViburDBCPDataSource;

import java.util.concurrent.CountDownLatch;

import static org.pak.dbq.internal.TestMessage.QUEUE_NAME;

@Testcontainers
@Slf4j
@Disabled("Manual performance test")
class QueueManagerPerformanceTest extends BaseIntegrationTest {
    private static final int MESSAGE_COUNT = 100_000;
    private static final int CONCURRENCY = 50;

    QueueManager queueManager;

    @BeforeEach
    void setUp() throws Exception {
        var viburDBCPDataSource = new ViburDBCPDataSource();
        viburDBCPDataSource.setJdbcUrl(postgres.getJdbcUrl());
        viburDBCPDataSource.setPoolMaxSize(50);
        viburDBCPDataSource.setUsername(postgres.getUsername());
        viburDBCPDataSource.setPassword(postgres.getPassword());
        viburDBCPDataSource.start();

        dataSource = viburDBCPDataSource;
        springTransactionService = setupSpringTransactionService(dataSource);
        jdbcTemplate = setupJdbcTemplate(dataSource);
        persistenceService = setupPersistenceService(jdbcTemplate);
        jsonbConverter = setupJsonbConverter();
        pgQueryService = setupQueryService(persistenceService, jsonbConverter);
        tableManager = setupTableManager(pgQueryService);

        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1);

        queueManager = new QueueManager(new PgQueryService(persistenceService, TEST_SCHEMA, jsonbConverter),
                springTransactionService, new SimpleMessageFactory(), tableManager);
    }

    @Test
    void performanceTest() throws Exception {
        queueManager.registerQueue(QueueConfig.builder()
                .queueName(QUEUE_NAME)
                .build());

        var producer = queueManager.registerProducer(ProducerConfig.<TestMessage>builder()
                .queueName(QUEUE_NAME)
                .clazz(TestMessage.class)
                .build());

        var countDownLatch = new CountDownLatch(MESSAGE_COUNT);

        queueManager.registerConsumer(ConsumerConfig.<TestMessage>builder()
                .messageHandler(message -> countDownLatch.countDown())
                .queueName(QUEUE_NAME)
                .subscriptionId(SUBSCRIPTION_NAME_1)
                .properties(ConsumerConfig.Properties.builder()
                        .concurrency(CONCURRENCY)
                        .build())
                .build());

        queueManager.startConsumers();

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            producer.send(new TestMessage("test-name"));
        }

        countDownLatch.await();
        queueManager.stopConsumers();
    }
}
