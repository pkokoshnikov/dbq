package org.pak.messagebus.core;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.pak.messagebus.pg.PgQueryService;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.vibur.dbcp.ViburDBCPDataSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.messagebus.core.TestMessage.MESSAGE_NAME;

@Testcontainers
@Slf4j
class MessageBusIntegrationTest extends BaseIntegrationTest {
    MessageBus messageBus;

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
        messagePublisherFactory = setupMessagePublisherFactory(pgQueryService);
        messageProcessorFactory = setupMessageProcessorFactory(pgQueryService, springTransactionService);

        createMessageTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1);
        createSubscriptionTable(SUBSCRIPTION_NAME_2);
        tableManager.registerMessage(MESSAGE_NAME, 30);
        tableManager.registerSubscription(MESSAGE_NAME, SUBSCRIPTION_NAME_1, 30);
        tableManager.registerSubscription(MESSAGE_NAME, SUBSCRIPTION_NAME_2, 30);

        messageBus = new MessageBus(new PgQueryService(persistenceService, TEST_SCHEMA, jsonbConverter),
                springTransactionService, new StdMessageFactory());
    }

    @Test
    void publishSubscribeTest() throws InterruptedException {
        messageBus.registerPublisher(PublisherConfig.<TestMessage>builder()
                .messageName(MESSAGE_NAME)
                .clazz(TestMessage.class)
                .properties(PublisherConfig.Properties.builder()
                        .storageDays(10)
                        .build())
                .build());

        var countDownLatch = new CountDownLatch(2);
        var reference1 = new AtomicReference<TestMessage>();
        var reference2 = new AtomicReference<TestMessage>();

        messageBus.registerSubscriber(SubscriberConfig.<TestMessage>builder()
                .messageListener(message -> {
                    reference1.set(message.payload());
                    countDownLatch.countDown();
                })
                .messageName(MESSAGE_NAME)
                .subscriptionName(SUBSCRIPTION_NAME_1)
                .build());

        messageBus.registerSubscriber(SubscriberConfig.<TestMessage>builder()
                .messageListener(message -> {
                    reference2.set(message.payload());
                    countDownLatch.countDown();
                })
                .messageName(MESSAGE_NAME)
                .subscriptionName(SUBSCRIPTION_NAME_2)
                .build());

        messageBus.startSubscribers();
        TestMessage testMessage = new TestMessage("test-name");
        messageBus.publish(testMessage);

        countDownLatch.await();
        messageBus.stopSubscribers();

        var handledMessage1 = reference1.get();
        assertThat(handledMessage1).isEqualTo(testMessage);
        var handledMessage2 = reference2.get();
        assertThat(handledMessage2).isEqualTo(testMessage);
    }

    //only for manual running
    @Test
    @Disabled
    void performanceTest() throws InterruptedException {
        messageBus.registerPublisher(PublisherConfig.<TestMessage>builder()
                .messageName(MESSAGE_NAME)
                .build());

        var countDownLatch = new CountDownLatch(100_000);

        messageBus.registerSubscriber(SubscriberConfig.<TestMessage>builder()
                .messageListener(message -> countDownLatch.countDown())
                .messageName(MESSAGE_NAME)
                .subscriptionName(SUBSCRIPTION_NAME_1)
                .properties(SubscriberConfig.Properties.builder()
                        .concurrency(50)
                        .build())
                .build());

        messageBus.startSubscribers();

        for (int i = 0; i < 100_000; i++) {
            TestMessage testMessage = new TestMessage("test-name");
            messageBus.publish(testMessage);
        }

        countDownLatch.await();
        messageBus.stopSubscribers();
    }
}
