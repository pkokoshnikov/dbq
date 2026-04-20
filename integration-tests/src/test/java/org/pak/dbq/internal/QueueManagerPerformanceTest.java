package org.pak.dbq.internal;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.Message;
import org.pak.dbq.api.ProducerConfig;
import org.pak.dbq.api.QueueConfig;
import org.pak.dbq.api.QueueManager;
import org.pak.dbq.internal.support.SimpleMessageFactory;
import org.pak.dbq.pg.PgQueryServiceFactory;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.vibur.dbcp.ViburDBCPDataSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.pak.dbq.internal.TestMessage.QUEUE_NAME;

@Testcontainers
@Slf4j
@Disabled("Manual performance test")
class QueueManagerPerformanceTest extends BaseIntegrationTest {
    private static final int MESSAGE_COUNT = 100_000;
    private static final int CONSUMER_COUNT = 6;
    private static final int CONCURRENCY_PER_CONSUMER = 10;
    private static final int TOTAL_CONCURRENCY = CONSUMER_COUNT * CONCURRENCY_PER_CONSUMER;

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
        pgQueryService = setupQueueService(persistenceService);
        tableManager = setupTableManager(persistenceService);

        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1);

        queueManager = new QueueManager(new PgQueryServiceFactory(
                persistenceService,
                TEST_SCHEMA,
                jsonbConverter),
                springTransactionService, new SimpleMessageFactory(), tableManager);
    }

    @Test
    void performanceTest() throws Exception {
        runPerformanceTest(false);
    }

    @Test
    void performanceTestWithKeyLock() throws Exception {
        runPerformanceTest(true);
    }

    private void runPerformanceTest(boolean serializedByKey) throws Exception {
        clearTables();
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1, false, serializedByKey);

        queueManager.registerQueue(QueueConfig.builder()
                .queueName(QUEUE_NAME)
                .build());

        var producer = queueManager.registerProducer(ProducerConfig.<TestMessage>builder()
                .queueName(QUEUE_NAME)
                .clazz(TestMessage.class)
                .build());

        var countDownLatch = new CountDownLatch(MESSAGE_COUNT);

        var consumerManagers = registerConsumerManagers(countDownLatch, serializedByKey);
        consumerManagers.forEach(QueueManager::startConsumers);

        var startedAt = System.nanoTime();
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            producer.send(new Message<>(
                    "perf-key-" + i,
                    Instant.now(),
                    new TestMessage("test-name")));
        }

        countDownLatch.await();
        var elapsedNanos = System.nanoTime() - startedAt;
        consumerManagers.forEach(QueueManager::stopConsumers);

        var elapsedMillis = TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
        var throughputPerSecond = MESSAGE_COUNT / (elapsedNanos / 1_000_000_000.0);
        log.info("""
                        QueueManagerPerformanceTest finished. serializedByKey={}, messages={}, consumerCount={}, \
                        concurrencyPerConsumer={}, totalConcurrency={}, elapsedMs={}, throughput={}""",
                serializedByKey,
                MESSAGE_COUNT,
                CONSUMER_COUNT,
                CONCURRENCY_PER_CONSUMER,
                TOTAL_CONCURRENCY,
                elapsedMillis,
                Math.round(throughputPerSecond));
    }

    private List<QueueManager> registerConsumerManagers(
            CountDownLatch countDownLatch,
            boolean serializedByKey
    ) throws Exception {
        var consumerManagers = new ArrayList<QueueManager>(CONSUMER_COUNT);

        for (int i = 0; i < CONSUMER_COUNT; i++) {
            var consumerQueueManager = new QueueManager(new PgQueryServiceFactory(
                    persistenceService,
                    TEST_SCHEMA,
                    jsonbConverter),
                    springTransactionService, new SimpleMessageFactory(), tableManager);

            consumerQueueManager.registerQueue(QueueConfig.builder()
                    .queueName(QUEUE_NAME)
                    .build());

            consumerQueueManager.registerConsumer(ConsumerConfig.<TestMessage>builder()
                    .messageHandler(message -> countDownLatch.countDown())
                    .queueName(QUEUE_NAME)
                    .subscriptionId(SUBSCRIPTION_NAME_1)
                    .properties(ConsumerConfig.Properties.builder()
                            .concurrency(CONCURRENCY_PER_CONSUMER)
                            .serializedByKey(serializedByKey)
                            .build())
                    .build());

            consumerManagers.add(consumerQueueManager);
        }

        return List.copyOf(consumerManagers);
    }
}
