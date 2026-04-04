package org.pak.qdb.internal;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.pak.qdb.api.ConsumerConfig;
import org.pak.qdb.api.ProducerConfig;
import org.pak.qdb.api.QueueManager;
import org.pak.qdb.support.StdMessageFactory;
import org.pak.qdb.pg.PgQueryService;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.vibur.dbcp.ViburDBCPDataSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.qdb.internal.TestMessage.QUEUE_NAME;

@Testcontainers
@Slf4j
class QueueManagerIntegrationTest extends BaseIntegrationTest {
    QueueManager queue;

    @BeforeEach
    void setUp() {
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
        schemaSqlGenerator = setupSchemaSqlGenerator();
        pgQueryService = setupQueryService(persistenceService, jsonbConverter);
        tableManager = setupTableManager(pgQueryService);
        producerFactory = setupProducerFactory(pgQueryService);
        queueProcessorFactory = setupQueueProcessorFactory(pgQueryService, springTransactionService);

        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1);
        createSubscriptionTable(SUBSCRIPTION_NAME_2);
        tableManager.registerQueue(QUEUE_NAME, 30);
        tableManager.registerSubscription(QUEUE_NAME, SUBSCRIPTION_NAME_1, 30);
        tableManager.registerSubscription(QUEUE_NAME, SUBSCRIPTION_NAME_2, 30);

        queue = new QueueManager(new PgQueryService(persistenceService, TEST_SCHEMA, jsonbConverter),
                springTransactionService, new StdMessageFactory());
    }

    @Test
    void publishSubscribeTest() throws InterruptedException {
        var producer = queue.registerProducer(ProducerConfig.<TestMessage>builder()
                .queueName(QUEUE_NAME)
                .clazz(TestMessage.class)
                .properties(ProducerConfig.Properties.builder()
                        .storageDays(10)
                        .build())
                .build());

        var countDownLatch = new CountDownLatch(2);
        var reference1 = new AtomicReference<TestMessage>();
        var reference2 = new AtomicReference<TestMessage>();

        queue.registerConsumer(ConsumerConfig.<TestMessage>builder()
                .messageHandler(message -> {
                    reference1.set(message.payload());
                    countDownLatch.countDown();
                })
                .queueName(QUEUE_NAME)
                .subscriptionId(SUBSCRIPTION_NAME_1)
                .build());

        queue.registerConsumer(ConsumerConfig.<TestMessage>builder()
                .messageHandler(message -> {
                    reference2.set(message.payload());
                    countDownLatch.countDown();
                })
                .queueName(QUEUE_NAME)
                .subscriptionId(SUBSCRIPTION_NAME_2)
                .build());

        queue.startConsumers();
        TestMessage testMessage = new TestMessage("test-name");
        producer.send(testMessage);

        countDownLatch.await();
        queue.stopConsumers();

        var handledMessage1 = reference1.get();
        assertThat(handledMessage1).isEqualTo(testMessage);
        var handledMessage2 = reference2.get();
        assertThat(handledMessage2).isEqualTo(testMessage);
    }

    //only for manual running
    @Test
    @Disabled
    void performanceTest() throws InterruptedException {
        var producer = queue.registerProducer(ProducerConfig.<TestMessage>builder()
                .queueName(QUEUE_NAME)
                .clazz(TestMessage.class)
                .build());

        var countDownLatch = new CountDownLatch(100_000);

        queue.registerConsumer(ConsumerConfig.<TestMessage>builder()
                .messageHandler(message -> countDownLatch.countDown())
                .queueName(QUEUE_NAME)
                .subscriptionId(SUBSCRIPTION_NAME_1)
                .properties(ConsumerConfig.Properties.builder()
                        .concurrency(50)
                        .build())
                .build());

        queue.startConsumers();

        for (int i = 0; i < 100_000; i++) {
            TestMessage testMessage = new TestMessage("test-name");
            producer.send(testMessage);
        }

        countDownLatch.await();
        queue.stopConsumers();
    }
}
