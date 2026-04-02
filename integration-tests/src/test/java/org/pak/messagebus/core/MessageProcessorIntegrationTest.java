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
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static java.util.Optional.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.messagebus.core.Status.FAILED;
import static org.pak.messagebus.core.Status.PROCESSED;
import static org.pak.messagebus.core.TestMessage.MESSAGE_NAME;

@Testcontainers
@Slf4j
class MessageProcessorIntegrationTest extends BaseIntegrationTest {
    @BeforeEach
    void setup() {
        dataSource = setupDatasource();
        springTransactionService = setupSpringTransactionService(dataSource);
        jdbcTemplate = setupJdbcTemplate(dataSource);
        persistenceService = setupPersistenceService(jdbcTemplate);
        jsonbConverter = setupJsonbConverter();
        pgQueryService = setupQueryService(persistenceService, jsonbConverter);
        tableManager = setupTableManager(pgQueryService);
        messagePublisherFactory = setupMessagePublisherFactory(pgQueryService);
        messageProcessorFactory = setupMessageProcessorFactory(pgQueryService, springTransactionService);
        queueMessagePublisherFactory = setupQueueMessagePublisherFactory(pgQueryService, springTransactionService);

        tableManager.registerMessage(MESSAGE_NAME, 30);
        tableManager.registerSubscription(MESSAGE_NAME, SUBSCRIPTION_NAME_1, 30);
    }

    @AfterEach
    void clear() {
        clearTables();
    }

    @Test
    void testSubmitMessage() {
        TestMessage testMessage = new TestMessage(TEST_VALUE);

        var messagePublisher = messagePublisherFactory.build().create();
        messagePublisher.publish(testMessage);

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
    void testSubmitBatchMessages() {
        var testMessage1 = new StdMessage<>(UUID.randomUUID().toString(), Instant.now(),
                new TestMessage(TEST_VALUE + "_1"));
        var testMessage2 = new StdMessage<>(UUID.randomUUID().toString(), Instant.now(),
                new TestMessage(TEST_VALUE + "_2"));
        var testMessage3 = new StdMessage<>(UUID.randomUUID().toString(), Instant.now(),
                new TestMessage(TEST_VALUE + "_3"));

        List<Message<TestMessage>> messages = List.of(testMessage1, testMessage2, testMessage3);
        QueueMessagePublisher<TestMessage> queueMessagePublisher = queueMessagePublisherFactory.build().create();
        queueMessagePublisher.publish(messages);
        queueMessagePublisher.publish(messages);//check duplicates

        var testMessagesContainers = selectTestMessages(SUBSCRIPTION_NAME_1);

        assertThat(testMessagesContainers).hasSize(3);

        messages.forEach(message -> {
            var testMessageContainer = testMessagesContainers.stream()
                    .filter(tmc -> tmc.getKey().equals(message.key()))
                    .findFirst()
                    .get();

            assertThat(testMessageContainer.getPayload()).isEqualTo(message.payload());
            assertThat(testMessageContainer.getCreated()).isNotNull();
            assertThat(testMessageContainer.getOriginatedTime()).isEqualTo(message.originatedTime());
            assertThat(testMessageContainer.getUpdated()).isNull();
            assertThat(testMessageContainer.getExecuteAfter()).isNotNull();
            assertThat(testMessageContainer.getAttempt()).isEqualTo(0);
            assertThat(testMessageContainer.getErrorMessage()).isNull();
            assertThat(testMessageContainer.getStackTrace()).isNull();
        });
    }

    @Test
    void testSuccessHandle() throws RetrayablePersistenceException {
        var messageProcessor = messageProcessorFactory.build().create();

        var messagePublisher = messagePublisherFactory.build().create();

        TestMessage testMessage1 = new TestMessage(TEST_VALUE);
        TestMessage testMessage2 = new TestMessage(TEST_VALUE + "-2");
        messagePublisher.publish(testMessage1);
        messagePublisher.publish(testMessage2);

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
        var messageProcessor = messageProcessorFactory.messageListener(testMessage -> {
                    throw new NonRetryableApplicationException(TEST_EXCEPTION_MESSAGE);
                })
                .nonRetryablePolicy(
                        exception -> NonRetryableApplicationException.class.isAssignableFrom(exception.getClass()))
                .build().create();

        var messagePublisher = messagePublisherFactory.build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        messagePublisher.publish(testMessage);
        messageProcessor.poolAndProcess();

        var testMessageContainer = hasSize1AndGetFirstHistory(selectTestMessagesFromHistory(SUBSCRIPTION_NAME_1));

        assertThat(testMessageContainer.getStatus()).isEqualTo(FAILED);
        assertThat(testMessageContainer.getAttempt()).isEqualTo(0);
        assertThat(testMessageContainer.getErrorMessage()).isEqualTo(TEST_EXCEPTION_MESSAGE);
        assertThat(testMessageContainer.getStackTrace()).isNotNull();
    }

    @Test
    void testHandleRetryableException() {
        var messageProcessor = messageProcessorFactory.messageListener(testMessage -> {
                    throw new RetryableApplicationException(TEST_EXCEPTION_MESSAGE);
                })
                .retryablePolicy((e, attempt) -> of(Duration.ofSeconds(600)))
                .build().create();

        var messagePublisher = messagePublisherFactory.build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        messagePublisher.publish(testMessage);

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
        var messageProcessor = messageProcessorFactory.messageListener(testMessage -> {
                    throw new RetryableApplicationException(TEST_EXCEPTION_MESSAGE);
                })
                .retryablePolicy((e, attempt) -> of(Duration.ofSeconds(0)))
                .build().create();

        var messagePublisher = messagePublisherFactory.build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        messagePublisher.publish(testMessage);

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
        var messageProcessor = messageProcessorFactory.messageListener(testMessage -> {
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

        var messagePublisher = messagePublisherFactory.build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        messagePublisher.publish(testMessage);

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
        var messagePublisher = messagePublisherFactory.build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        var key = UUID.randomUUID().toString();
        var originatedTime = Instant.now();
        messagePublisher.publish(new StdMessage<>(key, originatedTime, testMessage));
        messagePublisher.publish(new StdMessage<>(key, originatedTime, testMessage));

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
        var messagePublisher = messagePublisherFactory.build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        var timeout = postgresqlProxy.toxics().timeout("pg-timeout", ToxicDirection.DOWNSTREAM, 1000);

        Assertions.assertThrows(RetrayablePersistenceException.class, () -> messagePublisher.publish(testMessage));

        timeout.remove();
    }

    @Test
    void testProcessTimeoutException() throws IOException {
        var messagePublisher = messagePublisherFactory.build().create();

        var messageProcessor = messageProcessorFactory.build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        messagePublisher.publish(testMessage);

        var timeout = postgresqlProxy.toxics().timeout("pg-timeout", ToxicDirection.DOWNSTREAM, 1000);

        Assertions.assertThrows(RetrayablePersistenceException.class, messageProcessor::poolAndProcess);

        timeout.remove();
    }

    @Test
    void testBlockingException() {
        var messagePublisher = messagePublisherFactory.build().create();

        var messageProcessor = messageProcessorFactory.messageListener(testMessage -> {
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
        messagePublisher.publish(testMessage);

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
