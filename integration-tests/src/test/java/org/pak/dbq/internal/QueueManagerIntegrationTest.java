package org.pak.dbq.internal;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.ProducerConfig;
import org.pak.dbq.api.QueueManager;
import org.pak.dbq.internal.support.SimpleMessageFactory;
import org.pak.dbq.pg.PgQueryService;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.vibur.dbcp.ViburDBCPDataSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.dbq.internal.TestMessage.QUEUE_NAME;

@Testcontainers
@Slf4j
class QueueManagerIntegrationTest extends BaseIntegrationTest {
    QueueManager queueManager;

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
        consumerFactoryBuilder = setupQueueProcessorFactory(pgQueryService, springTransactionService);

        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1);
        createSubscriptionTable(SUBSCRIPTION_NAME_2);
        tableManager.registerQueue(QUEUE_NAME, 30);

        queueManager = new QueueManager(new PgQueryService(persistenceService, TEST_SCHEMA, jsonbConverter),
                springTransactionService, new SimpleMessageFactory());
    }

    @Test
    void publishSubscribeTest() throws InterruptedException {
        var producer = queueManager.registerProducer(ProducerConfig.<TestMessage>builder()
                .queueName(QUEUE_NAME)
                .clazz(TestMessage.class)
                        .properties(ProducerConfig.Properties.builder()
                        .retentionDays(10)
                        .build())
                .build());

        var countDownLatch = new CountDownLatch(2);
        var reference1 = new AtomicReference<TestMessage>();
        var reference2 = new AtomicReference<TestMessage>();

        queueManager.registerConsumer(ConsumerConfig.<TestMessage>builder()
                .messageHandler(message -> {
                    reference1.set(message.payload());
                    countDownLatch.countDown();
                })
                .queueName(QUEUE_NAME)
                .subscriptionId(SUBSCRIPTION_NAME_1)
                .build());

        queueManager.registerConsumer(ConsumerConfig.<TestMessage>builder()
                .messageHandler(message -> {
                    reference2.set(message.payload());
                    countDownLatch.countDown();
                })
                .queueName(QUEUE_NAME)
                .subscriptionId(SUBSCRIPTION_NAME_2)
                .build());

        queueManager.startConsumers();
        TestMessage testMessage = new TestMessage("test-name");
        producer.send(testMessage);

        countDownLatch.await();
        queueManager.stopConsumers();

        var handledMessage1 = reference1.get();
        assertThat(handledMessage1).isEqualTo(testMessage);
        var handledMessage2 = reference2.get();
        assertThat(handledMessage2).isEqualTo(testMessage);
    }
}
