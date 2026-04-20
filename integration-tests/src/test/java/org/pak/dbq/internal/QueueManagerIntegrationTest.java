package org.pak.dbq.internal;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.ProducerConfig;
import org.pak.dbq.api.QueueConfig;
import org.pak.dbq.api.QueueManager;
import org.pak.dbq.internal.support.SimpleMessageFactory;
import org.pak.dbq.pg.PgQueryServiceFactory;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.vibur.dbcp.ViburDBCPDataSource;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.dbq.internal.TestMessage.QUEUE_NAME;

@Testcontainers
@Slf4j
class QueueManagerIntegrationTest extends BaseIntegrationTest {
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
        pgQueryService = setupQueryService(persistenceService);
        tableManager = setupTableManager(persistenceService);
        producerFactory = setupProducerFactory(persistenceService, jsonbConverter);
        consumerFactoryBuilder = setupQueueProcessorFactory(
                springTransactionService,
                persistenceService,
                jsonbConverter);

        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1, true);
        createSubscriptionTable(SUBSCRIPTION_NAME_2);

        queueManager = new QueueManager(new PgQueryServiceFactory(
                persistenceService,
                TEST_SCHEMA,
                jsonbConverter),
                springTransactionService, new SimpleMessageFactory(), tableManager);
    }

    @Test
    void registerProducerAndConsumerPropagateRetentionToTableManager() throws Exception {
        queueManager.registerQueue(QueueConfig.builder()
                .queueName(QUEUE_NAME)
                .properties(QueueConfig.Properties.builder()
                        .retentionDays(10)
                        .build())
                .build());

        queueManager.registerProducer(ProducerConfig.<TestMessage>builder()
                .queueName(QUEUE_NAME)
                .clazz(TestMessage.class)
                .build());

        queueManager.registerConsumer(ConsumerConfig.<TestMessage>builder()
                .queueName(QUEUE_NAME)
                .subscriptionId(SUBSCRIPTION_NAME_1)
                .messageHandler(message -> {
                })
                .properties(ConsumerConfig.Properties.builder()
                        .historyEnabled(true)
                        .build())
                .build());

        assertThat(selectPartitions(QUEUE_TABLE)).hasSize(2);
        assertThat(selectPartitions(SUBSCRIPTION_TABLE_1_HISTORY)).hasSize(2);
    }

    @Test
    void publishSubscribeTest() throws Exception {
        queueManager.registerQueue(QueueConfig.builder()
                .queueName(QUEUE_NAME)
                .properties(QueueConfig.Properties.builder()
                        .retentionDays(10)
                        .build())
                .build());

        var producer = queueManager.registerProducer(ProducerConfig.<TestMessage>builder()
                .queueName(QUEUE_NAME)
                .clazz(TestMessage.class)
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

    @Test
    void publishBatchSubscribeTest() throws Exception {
        queueManager.registerQueue(QueueConfig.builder()
                .queueName(QUEUE_NAME)
                .properties(QueueConfig.Properties.builder()
                        .retentionDays(10)
                        .build())
                .build());

        var producer = queueManager.registerProducer(ProducerConfig.<TestMessage>builder()
                .queueName(QUEUE_NAME)
                .clazz(TestMessage.class)
                .build());

        var countDownLatch = new CountDownLatch(2);
        var handledMessages = new CopyOnWriteArrayList<TestMessage>();

        queueManager.registerConsumer(ConsumerConfig.<TestMessage>builder()
                .batchMessageHandler((records, acknowledger) -> {
                    for (var record : records) {
                        handledMessages.add(record.message().payload());
                        acknowledger.complete(record);
                        countDownLatch.countDown();
                    }
                })
                .queueName(QUEUE_NAME)
                .subscriptionId(SUBSCRIPTION_NAME_1)
                .properties(ConsumerConfig.Properties.builder()
                        .maxPollRecords(10)
                        .build())
                .build());

        TestMessage firstMessage = new TestMessage("test-name-1");
        TestMessage secondMessage = new TestMessage("test-name-2");
        producer.send(firstMessage);
        producer.send(secondMessage);

        queueManager.startConsumers();
        countDownLatch.await();
        queueManager.stopConsumers();

        assertThat(handledMessages).containsExactly(firstMessage, secondMessage);
    }
}
