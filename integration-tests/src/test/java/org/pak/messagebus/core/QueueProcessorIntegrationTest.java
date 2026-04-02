package org.pak.messagebus.core;


import eu.rekawek.toxiproxy.model.ToxicDirection;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pak.messagebus.core.error.RetrayablePersistenceException;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static java.util.Optional.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.messagebus.core.Status.FAILED;
import static org.pak.messagebus.core.Status.PROCESSED;
import static org.pak.messagebus.core.TestMessage.QUEUE_NAME;

@Testcontainers
@Slf4j
class QueueProcessorIntegrationTest extends BaseIntegrationTest {
    @BeforeEach
    void setup() {
        dataSource = setupDatasource();
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
        tableManager.registerQueue(QUEUE_NAME, 30);
        tableManager.registerSubscription(QUEUE_NAME, SUBSCRIPTION_NAME_1, 30);
    }

    @AfterEach
    void clear() {
        clearTables();
    }

    @Test
    void testSubmitMessage() {
        TestMessage testMessage = new TestMessage(TEST_VALUE);

        var producer = producerFactory.build().create();
        producer.publish(testMessage);

        var testMessageContainer = hasSize1AndGetFirst(selectTestMessages(SUBSCRIPTION_NAME_1));

        assertThat(testMessageContainer.getPayload()).isEqualTo(testMessage);
        assertThat(testMessageContainer.getCreated()).isNotNull();
        assertThat(testMessageContainer.getOriginatedTime()).isNotNull();
        assertThat(testMessageContainer.getUpdated()).isNull();
        assertThat(testMessageContainer.getExecuteAfter()).isNotNull();
        assertThat(testMessageContainer.getAttempt()).isEqualTo(0);
        assertThat(testMessageContainer.getErrorMessage()).isNull();
        assertThat(testMessageContainer.getStackTrace()).isNull();
    }

    @Test
    void testSuccessHandle() throws RetrayablePersistenceException {
        var messageProcessor = queueProcessorFactory.build().create();

        var producer = producerFactory.build().create();

        TestMessage testMessage1 = new TestMessage(TEST_VALUE);
        TestMessage testMessage2 = new TestMessage(TEST_VALUE + "-2");
        producer.publish(testMessage1);
        producer.publish(testMessage2);

        assertThat(messageProcessor.poolAndProcess()).isTrue();
        assertThat(messageProcessor.poolAndProcess()).isTrue();
        assertThat(messageProcessor.poolAndProcess()).isFalse();

        var testMessages = selectTestMessagesFromHistory(SUBSCRIPTION_NAME_1);

        assertThat(testMessages).hasSize(2);
        assertThat(testMessages.get(0).getStatus()).isEqualTo(PROCESSED);
        assertThat(testMessages.get(0).getMessage()).isEqualTo(testMessage1);

        assertThat(testMessages.get(1).getStatus()).isEqualTo(PROCESSED);
        assertThat(testMessages.get(1).getMessage()).isEqualTo(testMessage2);
    }

    @Test
    void testHandleNonRetryableException() {
        var messageProcessor = queueProcessorFactory.consumer(testMessage -> {
                    throw new NonRetryableApplicationException(TEST_EXCEPTION_MESSAGE);
                })
                .nonRetryablePolicy(
                        exception -> NonRetryableApplicationException.class.isAssignableFrom(exception.getClass()))
                .build().create();

        var producer = producerFactory.build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        producer.publish(testMessage);
        messageProcessor.poolAndProcess();

        var testMessageContainer = hasSize1AndGetFirstHistory(selectTestMessagesFromHistory(SUBSCRIPTION_NAME_1));

        assertThat(testMessageContainer.getStatus()).isEqualTo(FAILED);
        assertThat(testMessageContainer.getAttempt()).isEqualTo(0);
        assertThat(testMessageContainer.getErrorMessage()).isEqualTo(TEST_EXCEPTION_MESSAGE);
        assertThat(testMessageContainer.getStackTrace()).isNotNull();
    }

    @Test
    void testHandleRetryableException() {
        var messageProcessor = queueProcessorFactory.consumer(testMessage -> {
                    throw new RetryableApplicationException(TEST_EXCEPTION_MESSAGE);
                })
                .retryablePolicy((e, attempt) -> of(Duration.ofSeconds(600)))
                .build().create();

        var producer = producerFactory.build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        producer.publish(testMessage);

        var testMessageBeforeHandleContainer = hasSize1AndGetFirst(selectTestMessages(SUBSCRIPTION_NAME_1));

        messageProcessor.poolAndProcess();

        var testMessageContainer = hasSize1AndGetFirst(selectTestMessages(SUBSCRIPTION_NAME_1));

        assertThat(testMessageContainer.getAttempt()).isEqualTo(1);
        assertThat(testMessageContainer.getErrorMessage()).isEqualTo(TEST_EXCEPTION_MESSAGE);
        assertThat(testMessageContainer.getStackTrace()).isNotNull();
        assertThat(testMessageContainer.getUpdated()).isNotNull();
        assertThat(testMessageContainer.getExecuteAfter()).isAfterOrEqualTo(
                testMessageBeforeHandleContainer.getExecuteAfter().plus(Duration.ofSeconds(600)));

        messageProcessor.poolAndProcess();

        testMessageContainer = hasSize1AndGetFirst(selectTestMessages(SUBSCRIPTION_NAME_1));

        //check that it wasn't retried before executeAfter
        assertThat(testMessageContainer.getAttempt()).isEqualTo(1);
        assertThat(testMessageContainer.getErrorMessage()).isEqualTo(TEST_EXCEPTION_MESSAGE);
        assertThat(testMessageContainer.getStackTrace()).isNotNull();
        assertThat(testMessageContainer.getUpdated()).isNotNull();
        assertThat(testMessageContainer.getExecuteAfter()).isAfterOrEqualTo(
                testMessageBeforeHandleContainer.getExecuteAfter().plus(Duration.ofSeconds(60)));
    }

    @Test
    void testHandleRetryableException2() {
        var messageProcessor = queueProcessorFactory.consumer(testMessage -> {
                    throw new RetryableApplicationException(TEST_EXCEPTION_MESSAGE);
                })
                .retryablePolicy((e, attempt) -> of(Duration.ofSeconds(0)))
                .build().create();

        var producer = producerFactory.build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        producer.publish(testMessage);

        var testMessages = selectTestMessages(SUBSCRIPTION_NAME_1);
        assertThat(testMessages).hasSize(1);

        messageProcessor.poolAndProcess();

        var testMessageContainer = hasSize1AndGetFirst(selectTestMessages(SUBSCRIPTION_NAME_1));

        assertThat(testMessageContainer.getAttempt()).isEqualTo(1);
        assertThat(testMessageContainer.getErrorMessage()).isEqualTo(TEST_EXCEPTION_MESSAGE);
        assertThat(testMessageContainer.getStackTrace()).isNotNull();
        assertThat(testMessageContainer.getUpdated()).isNotNull();

        messageProcessor.poolAndProcess();

        testMessageContainer = hasSize1AndGetFirst(selectTestMessages(SUBSCRIPTION_NAME_1));

        assertThat(testMessageContainer.getAttempt()).isEqualTo(2);
        assertThat(testMessageContainer.getErrorMessage()).isEqualTo(TEST_EXCEPTION_MESSAGE);
        assertThat(testMessageContainer.getStackTrace()).isNotNull();
        assertThat(testMessageContainer.getUpdated()).isNotNull();
    }

    @Test
    void testHandleRetryableExceptionFail() {
        var messageProcessor = queueProcessorFactory.consumer(testMessage -> {
                    throw new RetryableApplicationException(TEST_EXCEPTION_MESSAGE);
                })
                .retryablePolicy((e, attempt) -> {
                    if (attempt == 0) {
                        return Optional.of(Duration.ofSeconds(0));
                    } else {
                        return Optional.empty();
                    }
                })
                .build().create();

        var producer = producerFactory.build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        producer.publish(testMessage);

        var testMessages = selectTestMessages(SUBSCRIPTION_NAME_1);
        assertThat(testMessages).hasSize(1);

        messageProcessor.poolAndProcess();

        var testMessageContainer = hasSize1AndGetFirst(selectTestMessages(SUBSCRIPTION_NAME_1));

        assertThat(testMessageContainer.getAttempt()).isEqualTo(1);
        assertThat(testMessageContainer.getErrorMessage()).isEqualTo(TEST_EXCEPTION_MESSAGE);
        assertThat(testMessageContainer.getStackTrace()).isNotNull();
        assertThat(testMessageContainer.getUpdated()).isNotNull();

        messageProcessor.poolAndProcess();

        var testMessageHistoryContainer = hasSize1AndGetFirstHistory(selectTestMessagesFromHistory(SUBSCRIPTION_NAME_1));

        assertThat(testMessageHistoryContainer.getStatus()).isEqualTo(FAILED);
        assertThat(testMessageHistoryContainer.getErrorMessage()).isEqualTo(TEST_EXCEPTION_MESSAGE);
        assertThat(testMessageHistoryContainer.getStackTrace()).isNotNull();
    }

    @Test
    void testDuplicateKeyPublish() {
        var producer = producerFactory.build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        var key = UUID.randomUUID().toString();
        var originatedTime = Instant.now();
        Message<TestMessage> message = new StdMessage<>(key, originatedTime, testMessage);
        producer.publish(message);
        producer.publish(message);

        var testMessageContainer = hasSize1AndGetFirst(selectTestMessages(SUBSCRIPTION_NAME_1));

        assertThat(testMessageContainer.getPayload()).isEqualTo(testMessage);
        assertThat(testMessageContainer.getCreated()).isNotNull();
        assertThat(testMessageContainer.getUpdated()).isNull();
        assertThat(testMessageContainer.getExecuteAfter()).isNotNull();
        assertThat(testMessageContainer.getAttempt()).isEqualTo(0);
        assertThat(testMessageContainer.getErrorMessage()).isNull();
        assertThat(testMessageContainer.getStackTrace()).isNull();
    }

    @Test
    void testPublishTimeoutException() throws IOException {
        var producer = producerFactory.build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        var timeout = postgresqlProxy.toxics().timeout("pg-timeout", ToxicDirection.DOWNSTREAM, 1000);

        Assertions.assertThrows(RetrayablePersistenceException.class, () -> producer.publish(testMessage));

        timeout.remove();
    }

    @Test
    void testProcessTimeoutException() throws IOException {
        var producer = producerFactory.build().create();

        var messageProcessor = queueProcessorFactory.build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        producer.publish(testMessage);

        var timeout = postgresqlProxy.toxics().timeout("pg-timeout", ToxicDirection.DOWNSTREAM, 1000);

        Assertions.assertThrows(RetrayablePersistenceException.class, messageProcessor::poolAndProcess);

        timeout.remove();
    }

    @Test
    void testBlockingException() {
        var producer = producerFactory.build().create();

        var messageProcessor = queueProcessorFactory.consumer(testMessage -> {
                    throw new BlockingApplicationException(TEST_EXCEPTION_MESSAGE);
                })
                .blockingPolicy(new BlockingPolicy() {
                    @Override
                    public boolean isBlocked(Exception exception) {
                        return BlockingApplicationException.class.isAssignableFrom(exception.getClass());
                    }

                    @Override
                    public @NonNull Duration apply(Exception exception) {
                        return Duration.ofMillis(30_000);
                    }
                })
                .build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        producer.publish(testMessage);

        messageProcessor.poolAndProcess();

        var testMessageContainer = hasSize1AndGetFirst(selectTestMessages(SUBSCRIPTION_NAME_1));

        assertThat(testMessageContainer.getPayload()).isEqualTo(testMessage);
        assertThat(testMessageContainer.getCreated()).isNotNull();
        assertThat(testMessageContainer.getUpdated()).isNull();
        assertThat(testMessageContainer.getExecuteAfter()).isNotNull();
        assertThat(testMessageContainer.getAttempt()).isEqualTo(0);
        assertThat(testMessageContainer.getErrorMessage()).isNull();
        assertThat(testMessageContainer.getStackTrace()).isNull();
    }
}
