package org.pak.dbq.internal.consumer;


import eu.rekawek.toxiproxy.model.ToxicDirection;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.Message;
import org.pak.dbq.api.MessageRecord;
import org.pak.dbq.api.policy.BlockingPolicy;
import org.pak.dbq.internal.BaseIntegrationTest;
import org.pak.dbq.internal.TestMessage;
import org.pak.dbq.error.MessageDeserializationException;
import org.pak.dbq.error.RetryablePersistenceException;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;

import static java.util.Optional.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pak.dbq.internal.TestMessage.QUEUE_NAME;
import static org.pak.dbq.internal.persistence.Status.FAILED;
import static org.pak.dbq.internal.persistence.Status.PROCESSED;

@Testcontainers
@Slf4j
class ConsumerIntegrationTest extends BaseIntegrationTest {
    @BeforeEach
    void setup() throws Exception {
        dataSource = setupDatasource();
        springTransactionService = setupSpringTransactionService(dataSource);
        jdbcTemplate = setupJdbcTemplate(dataSource);
        persistenceService = setupPersistenceService(jdbcTemplate);
        jsonbConverter = setupJsonbConverter();
        pgQueryService = setupQueryService(persistenceService, jsonbConverter);
        tableManager = setupTableManager(persistenceService);
        producerFactory = setupProducerFactory(pgQueryService, persistenceService, jsonbConverter);
        consumerFactoryBuilder = setupQueueProcessorFactory(
                pgQueryService,
                springTransactionService,
                persistenceService,
                jsonbConverter);

        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1, true);
        tableManager.registerQueue(QUEUE_NAME, 30, false);
        tableManager.registerSubscription(QUEUE_NAME, SUBSCRIPTION_NAME_1, true, false);
    }

    @AfterEach
    void clear() {
        clearTables();
    }

    @Test
    void testSubmitMessage() throws Exception {
        TestMessage testMessage = new TestMessage(TEST_VALUE);

        var producer = producerFactory.build().create();
        producer.send(testMessage);

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
    void testSuccessHandle() throws Exception {
        var consumer = consumerFactoryBuilder.build().create();

        var producer = producerFactory.build().create();

        TestMessage testMessage1 = new TestMessage(TEST_VALUE);
        TestMessage testMessage2 = new TestMessage(TEST_VALUE + "-2");
        producer.send(testMessage1);
        producer.send(testMessage2);

        assertThat(consumer.poolAndProcess()).isTrue();
        assertThat(consumer.poolAndProcess()).isTrue();
        assertThat(consumer.poolAndProcess()).isFalse();

        var testMessages = selectTestMessagesFromHistory(SUBSCRIPTION_NAME_1);

        assertThat(testMessages).hasSize(2);
        assertThat(testMessages.get(0).getStatus()).isEqualTo(PROCESSED);
        assertThat(testMessages.get(0).getMessage()).isEqualTo(testMessage1);

        assertThat(testMessages.get(1).getStatus()).isEqualTo(PROCESSED);
        assertThat(testMessages.get(1).getMessage()).isEqualTo(testMessage2);
    }

    @Test
    void testBatchSuccessHandle() throws Exception {
        var handledRecords = new ArrayList<MessageRecord<TestMessage>>();
        var consumer = consumerFactoryBuilder
                .messageHandler(null)
                .batchMessageHandler((records, acknowledger) -> {
                    handledRecords.addAll(records);
                    for (var record : records) {
                        acknowledger.complete(record);
                    }
                })
                .properties(ConsumerConfig.Properties.builder()
                        .historyEnabled(true)
                        .maxPollRecords(10)
                        .build())
                .build()
                .create();

        var producer = producerFactory.build().create();
        TestMessage testMessage1 = new TestMessage(TEST_VALUE);
        TestMessage testMessage2 = new TestMessage(TEST_VALUE + "-2");
        producer.send(testMessage1);
        producer.send(testMessage2);

        assertThat(consumer.poolAndProcess()).isTrue();
        assertThat(consumer.poolAndProcess()).isFalse();

        assertThat(handledRecords).hasSize(2);
        assertThat(handledRecords).allSatisfy(record -> assertThat(record.id()).isNotNull());
        assertThat(handledRecords).extracting(record -> record.message().payload())
                .containsExactly(testMessage1, testMessage2);

        var testMessages = selectTestMessagesFromHistory(SUBSCRIPTION_NAME_1);
        assertThat(testMessages).hasSize(2);
        assertThat(testMessages.get(0).getStatus()).isEqualTo(PROCESSED);
        assertThat(testMessages.get(0).getMessage()).isEqualTo(testMessage1);
        assertThat(testMessages.get(1).getStatus()).isEqualTo(PROCESSED);
        assertThat(testMessages.get(1).getMessage()).isEqualTo(testMessage2);
    }

    @Test
    void testBatchRetryAndFail() throws Exception {
        var retryException = new RetryableApplicationException(TEST_EXCEPTION_MESSAGE);
        var failException = new NonRetryableApplicationException("batch-fail");
        var consumer = consumerFactoryBuilder
                .messageHandler(null)
                .batchMessageHandler((records, acknowledger) -> {
                    acknowledger.retry(records.get(0), Duration.ofSeconds(0), retryException);
                    acknowledger.fail(records.get(1), failException);
                })
                .properties(ConsumerConfig.Properties.builder()
                        .historyEnabled(true)
                        .maxPollRecords(10)
                        .build())
                .build()
                .create();

        var producer = producerFactory.build().create();
        TestMessage retryMessage = new TestMessage(TEST_VALUE);
        TestMessage failMessage = new TestMessage(TEST_VALUE + "-2");
        producer.send(retryMessage);
        producer.send(failMessage);

        assertThat(consumer.poolAndProcess()).isTrue();

        var pendingMessages = selectTestMessages(SUBSCRIPTION_NAME_1);
        assertThat(pendingMessages).hasSize(1);
        assertThat(pendingMessages.getFirst().getPayload()).isEqualTo(retryMessage);
        assertThat(pendingMessages.getFirst().getAttempt()).isEqualTo(1);
        assertThat(pendingMessages.getFirst().getErrorMessage()).isEqualTo(TEST_EXCEPTION_MESSAGE);

        var historyMessages = selectTestMessagesFromHistory(SUBSCRIPTION_NAME_1);
        assertThat(historyMessages).hasSize(1);
        assertThat(historyMessages.getFirst().getStatus()).isEqualTo(FAILED);
        assertThat(historyMessages.getFirst().getMessage()).isEqualTo(failMessage);
        assertThat(historyMessages.getFirst().getErrorMessage()).isEqualTo("batch-fail");
    }

    @Test
    void testHandleNonRetryableException() throws Exception {
        var consumer = consumerFactoryBuilder.messageHandler(testMessage -> {
                    throw new NonRetryableApplicationException(TEST_EXCEPTION_MESSAGE);
                })
                .nonRetryablePolicy(
                        exception -> NonRetryableApplicationException.class.isAssignableFrom(exception.getClass()))
                .build().create();

        var producer = producerFactory.build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        producer.send(testMessage);
        consumer.poolAndProcess();

        var testMessageContainer = hasSize1AndGetFirstHistory(selectTestMessagesFromHistory(SUBSCRIPTION_NAME_1));

        assertThat(testMessageContainer.getStatus()).isEqualTo(FAILED);
        assertThat(testMessageContainer.getAttempt()).isEqualTo(0);
        assertThat(testMessageContainer.getErrorMessage()).isEqualTo(TEST_EXCEPTION_MESSAGE);
        assertThat(testMessageContainer.getStackTrace()).isNotNull();
    }

    @Test
    void testHandleRetryableException() throws Exception {
        var consumer = consumerFactoryBuilder.messageHandler(testMessage -> {
                    throw new RetryableApplicationException(TEST_EXCEPTION_MESSAGE);
                })
                .retryablePolicy((e, attempt) -> of(Duration.ofSeconds(600)))
                .build().create();

        var producer = producerFactory.build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        producer.send(testMessage);

        var testMessageBeforeHandleContainer = hasSize1AndGetFirst(selectTestMessages(SUBSCRIPTION_NAME_1));

        consumer.poolAndProcess();

        var testMessageContainer = hasSize1AndGetFirst(selectTestMessages(SUBSCRIPTION_NAME_1));

        assertThat(testMessageContainer.getAttempt()).isEqualTo(1);
        assertThat(testMessageContainer.getErrorMessage()).isEqualTo(TEST_EXCEPTION_MESSAGE);
        assertThat(testMessageContainer.getStackTrace()).isNotNull();
        assertThat(testMessageContainer.getUpdated()).isNotNull();
        assertThat(testMessageContainer.getExecuteAfter()).isAfterOrEqualTo(
                testMessageBeforeHandleContainer.getExecuteAfter().plus(Duration.ofSeconds(600)));

        consumer.poolAndProcess();

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
    void testHandleRetryableException2() throws Exception {
        var consumer = consumerFactoryBuilder.messageHandler(testMessage -> {
                    throw new RetryableApplicationException(TEST_EXCEPTION_MESSAGE);
                })
                .retryablePolicy((e, attempt) -> of(Duration.ofSeconds(0)))
                .build().create();

        var producer = producerFactory.build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        producer.send(testMessage);

        var testMessages = selectTestMessages(SUBSCRIPTION_NAME_1);
        assertThat(testMessages).hasSize(1);

        consumer.poolAndProcess();

        var testMessageContainer = hasSize1AndGetFirst(selectTestMessages(SUBSCRIPTION_NAME_1));

        assertThat(testMessageContainer.getAttempt()).isEqualTo(1);
        assertThat(testMessageContainer.getErrorMessage()).isEqualTo(TEST_EXCEPTION_MESSAGE);
        assertThat(testMessageContainer.getStackTrace()).isNotNull();
        assertThat(testMessageContainer.getUpdated()).isNotNull();

        consumer.poolAndProcess();

        testMessageContainer = hasSize1AndGetFirst(selectTestMessages(SUBSCRIPTION_NAME_1));

        assertThat(testMessageContainer.getAttempt()).isEqualTo(2);
        assertThat(testMessageContainer.getErrorMessage()).isEqualTo(TEST_EXCEPTION_MESSAGE);
        assertThat(testMessageContainer.getStackTrace()).isNotNull();
        assertThat(testMessageContainer.getUpdated()).isNotNull();
    }

    @Test
    void testHandleRetryableExceptionFail() throws Exception {
        var consumer = consumerFactoryBuilder.messageHandler(testMessage -> {
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
        producer.send(testMessage);

        var testMessages = selectTestMessages(SUBSCRIPTION_NAME_1);
        assertThat(testMessages).hasSize(1);

        consumer.poolAndProcess();

        var testMessageContainer = hasSize1AndGetFirst(selectTestMessages(SUBSCRIPTION_NAME_1));

        assertThat(testMessageContainer.getAttempt()).isEqualTo(1);
        assertThat(testMessageContainer.getErrorMessage()).isEqualTo(TEST_EXCEPTION_MESSAGE);
        assertThat(testMessageContainer.getStackTrace()).isNotNull();
        assertThat(testMessageContainer.getUpdated()).isNotNull();

        consumer.poolAndProcess();

        var testMessageHistoryContainer = hasSize1AndGetFirstHistory(selectTestMessagesFromHistory(SUBSCRIPTION_NAME_1));

        assertThat(testMessageHistoryContainer.getStatus()).isEqualTo(FAILED);
        assertThat(testMessageHistoryContainer.getErrorMessage()).isEqualTo(TEST_EXCEPTION_MESSAGE);
        assertThat(testMessageHistoryContainer.getStackTrace()).isNotNull();
    }

    @Test
    void testDuplicateKeyPublish() throws Exception {
        var producer = producerFactory.build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        var key = UUID.randomUUID().toString();
        var originatedTime = Instant.now();
        Message<TestMessage> message = new Message<>(key, originatedTime, testMessage);
        producer.send(message);
        producer.send(message);

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
    void testPublishTimeoutException() throws Exception {
        var producer = producerFactory.build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        var timeout = postgresqlProxy.toxics().timeout("pg-timeout", ToxicDirection.DOWNSTREAM, 1000);

        Assertions.assertThrows(RetryablePersistenceException.class, () -> producer.send(testMessage));

        timeout.remove();
    }

    @Test
    void testProcessTimeoutException() throws Exception {
        var producer = producerFactory.build().create();

        var messageProcessor = consumerFactoryBuilder.build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        producer.send(testMessage);

        var timeout = postgresqlProxy.toxics().timeout("pg-timeout", ToxicDirection.DOWNSTREAM, 1000);

        Assertions.assertThrows(RetryablePersistenceException.class, messageProcessor::poolAndProcess);

        timeout.remove();
    }

    @Test
    void testBlockingException() throws Exception {
        var producer = producerFactory.build().create();

        var consumer = consumerFactoryBuilder.messageHandler(testMessage -> {
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
        producer.send(testMessage);

        consumer.poolAndProcess();

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
    void testProcessMessageDeserializationException() throws Exception {
        var producer = producerFactory.build().create();
        var consumer = consumerFactoryBuilder.build().create();

        var key = UUID.randomUUID().toString();
        var originatedTime = Instant.now();
        producer.send(new Message<>(key, originatedTime, new TestMessage(TEST_VALUE)));

        jdbcTemplate.update("""
                        UPDATE public.test_message
                        SET payload = ?::jsonb
                        WHERE key = ? AND originated_at = ?""",
                """
                        {"@type":"test-message","name":{"invalid":"value"}}""",
                key,
                java.time.OffsetDateTime.ofInstant(originatedTime, java.time.ZoneId.systemDefault()));

        assertThatThrownBy(consumer::poolAndProcess)
                .isInstanceOf(MessageDeserializationException.class);
    }

    @Test
    void testSuccessHandleWithoutHistory() throws Exception {
        clearTables();
        createQueueTable();
        createSubscriptionTable(SUBSCRIPTION_NAME_1, false);
        tableManager.registerQueue(QUEUE_NAME, 30, false);

        var consumer = consumerFactoryBuilder
                .properties(ConsumerConfig.Properties.builder()
                        .historyEnabled(false)
                        .build())
                .build()
                .create();
        var producer = producerFactory.build().create();

        producer.send(new TestMessage(TEST_VALUE));

        assertThat(consumer.poolAndProcess()).isTrue();
        assertThat(consumer.poolAndProcess()).isFalse();
        assertThat(selectTestMessages(SUBSCRIPTION_NAME_1)).isEmpty();
        assertThat(jdbcTemplate.queryForObject("SELECT to_regclass(?)", String.class,
                TEST_SCHEMA.value() + "." + SUBSCRIPTION_TABLE_1_HISTORY)).isNull();
    }
}
