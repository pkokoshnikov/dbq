package org.pak.dbq.internal;

import org.junit.jupiter.api.Test;
import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.MessageHandler;
import org.pak.dbq.api.ProducerConfig;
import org.pak.dbq.api.QueueConfig;
import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.QueueManager;
import org.pak.dbq.api.SubscriptionId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pak.dbq.internal.CoreTestSupport.QUEUE_NAME;

class QueueManagerTest {
    private static final SubscriptionId SUBSCRIPTION_ID = new SubscriptionId("test-subscription");

    @Test
    void registerProducerReturnsProducerThatStoresMessage() throws Exception {
        var queryServiceFactory = new CoreTestSupport.RecordingQueryServiceFactory();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var tableManager = new CoreTestSupport.RecordingTableManager();
        var queue = new QueueManager(queryServiceFactory, transactionService, tableManager);
        queue.registerQueue(QueueConfig.builder()
                .queueName(QUEUE_NAME)
                .build());

        var producer = queue.registerProducer(ProducerConfig.<String>builder()
                .queueName(QUEUE_NAME)
                .clazz(String.class)
                .build());

        producer.send("payload");

        assertThat(queryServiceFactory.getInserts()).hasSize(1);
        assertThat(queryServiceFactory.getInserts().getFirst().queueName()).isEqualTo(QUEUE_NAME);
        assertThat(queryServiceFactory.getInserts().getFirst().message().payload()).isEqualTo("payload");
    }

    @Test
    void registerProducerKeepsSeparateProducersForDifferentQueuesWithSamePayloadClass() throws Exception {
        var queryServiceFactory = new CoreTestSupport.RecordingQueryServiceFactory();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var queueManager = new QueueManager(queryServiceFactory, transactionService);
        var anotherQueue = new QueueName("another-queue");

        queueManager.registerQueue(QueueConfig.builder()
                .queueName(QUEUE_NAME)
                .build());
        queueManager.registerQueue(QueueConfig.builder()
                .queueName(anotherQueue)
                .build());

        var firstProducer = queueManager.registerProducer(ProducerConfig.<String>builder()
                .queueName(QUEUE_NAME)
                .clazz(String.class)
                .build());
        var secondProducer = queueManager.registerProducer(ProducerConfig.<String>builder()
                .queueName(anotherQueue)
                .clazz(String.class)
                .build());

        firstProducer.send("first");
        secondProducer.send("second");

        assertThat(queryServiceFactory.getInserts()).hasSize(2);
        assertThat(queryServiceFactory.getInserts().get(0).queueName()).isEqualTo(QUEUE_NAME);
        assertThat(queryServiceFactory.getInserts().get(1).queueName()).isEqualTo(anotherQueue);
    }

    @Test
    void registerProducerAllowsIdempotentRegistrationWithSameConfigInstance() throws Exception {
        var queryServiceFactory = new CoreTestSupport.RecordingQueryServiceFactory();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var queueManager = new QueueManager(queryServiceFactory, transactionService);
        queueManager.registerQueue(QueueConfig.builder()
                .queueName(QUEUE_NAME)
                .build());
        var producerConfig = ProducerConfig.<String>builder()
                .queueName(QUEUE_NAME)
                .clazz(String.class)
                .build();

        var firstProducer = queueManager.registerProducer(producerConfig);
        var secondProducer = queueManager.registerProducer(producerConfig);

        assertThat(secondProducer).isSameAs(firstProducer);
    }

    @Test
    void registerProducerFailsOnConflictingRegistration() throws Exception {
        var queryServiceFactory = new CoreTestSupport.RecordingQueryServiceFactory();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var queueManager = new QueueManager(queryServiceFactory, transactionService);
        queueManager.registerQueue(QueueConfig.builder()
                .queueName(QUEUE_NAME)
                .build());

        queueManager.registerProducer(ProducerConfig.<String>builder()
                .queueName(QUEUE_NAME)
                .clazz(String.class)
                .build());

        assertThatThrownBy(() -> queueManager.registerProducer(ProducerConfig.<String>builder()
                .queueName(QUEUE_NAME)
                .clazz(String.class)
                .messageContextPropagator(new CoreTestSupport.RecordingMessageContextPropagator(java.util.Map.of()))
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Producer is already registered");
    }

    @Test
    void registerProducerFailsWhenQueueIsNotInitialized() {
        var queryServiceFactory = new CoreTestSupport.RecordingQueryServiceFactory();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var queueManager = new QueueManager(queryServiceFactory, transactionService);

        assertThatThrownBy(() -> queueManager.registerProducer(ProducerConfig.<String>builder()
                .queueName(QUEUE_NAME)
                .clazz(String.class)
                .build()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Call registerQueue(...) first");
    }

    @Test
    void registerQueueRegistersQueueRetention() throws Exception {
        var queryServiceFactory = new CoreTestSupport.RecordingQueryServiceFactory();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var tableManager = new CoreTestSupport.RecordingTableManager();
        var queueManager = new QueueManager(queryServiceFactory, transactionService, tableManager);

        queueManager.registerQueue(QueueConfig.builder()
                .queueName(QUEUE_NAME)
                .properties(QueueConfig.Properties.builder()
                        .retentionDays(10)
                        .build())
                .build());

        assertThat(tableManager.getQueueRegistrations())
                .containsExactly(new CoreTestSupport.QueueRegistrationCall(QUEUE_NAME, 10, false));
    }

    @Test
    void registerQueuePropagatesAutoDdlFromQueueManagerProperties() throws Exception {
        var queryServiceFactory = new CoreTestSupport.RecordingQueryServiceFactory();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var tableManager = new CoreTestSupport.RecordingTableManager();
        var queueManager = new QueueManager(
                queryServiceFactory,
                transactionService,
                tableManager,
                QueueManager.Properties.builder()
                        .autoDdl(true)
                        .build());

        queueManager.registerQueue(QueueConfig.builder()
                .queueName(QUEUE_NAME)
                .build());

        assertThat(tableManager.getQueueRegistrations())
                .containsExactly(new CoreTestSupport.QueueRegistrationCall(QUEUE_NAME, 30, true));
    }

    @Test
    void registerConsumerAllowsIdempotentRegistrationWithSameConfigInstance() throws Exception {
        var queryServiceFactory = new CoreTestSupport.RecordingQueryServiceFactory();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var tableManager = new CoreTestSupport.RecordingTableManager();
        var queueManager = new QueueManager(queryServiceFactory, transactionService, tableManager);
        queueManager.registerQueue(QueueConfig.builder()
                .queueName(QUEUE_NAME)
                .build());
        MessageHandler<String> handler = message -> {
        };
        var consumerConfig = ConsumerConfig.<String>builder()
                .queueName(QUEUE_NAME)
                .subscriptionId(SUBSCRIPTION_ID)
                .messageHandler(handler)
                .build();

        queueManager.registerConsumer(consumerConfig);
        queueManager.registerConsumer(consumerConfig);

        assertThat(tableManager.getSubscriptionRegistrations())
                .containsExactly(new CoreTestSupport.SubscriptionRegistrationCall(
                        QUEUE_NAME,
                        SUBSCRIPTION_ID,
                        false,
                        false));
    }

    @Test
    void registerConsumerRegistersHistoryRetentionWhenQueueIsInitialized() throws Exception {
        var queryServiceFactory = new CoreTestSupport.RecordingQueryServiceFactory();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var tableManager = new CoreTestSupport.RecordingTableManager();
        var queueManager = new QueueManager(queryServiceFactory, transactionService, tableManager);

        queueManager.registerQueue(QueueConfig.builder()
                .queueName(QUEUE_NAME)
                .properties(QueueConfig.Properties.builder()
                        .retentionDays(7)
                        .build())
                .build());

        queueManager.registerConsumer(ConsumerConfig.<String>builder()
                .queueName(QUEUE_NAME)
                .subscriptionId(SUBSCRIPTION_ID)
                .messageHandler(message -> {
                })
                .properties(ConsumerConfig.Properties.builder()
                        .historyEnabled(true)
                        .build())
                .build());

        assertThat(tableManager.getQueueRegistrations())
                .containsExactly(new CoreTestSupport.QueueRegistrationCall(QUEUE_NAME, 7, false));
        assertThat(tableManager.getSubscriptionRegistrations())
                .containsExactly(new CoreTestSupport.SubscriptionRegistrationCall(
                        QUEUE_NAME,
                        SUBSCRIPTION_ID,
                        true,
                        false));
    }

    @Test
    void registerConsumerFailsWhenQueueIsNotInitialized() {
        var queryServiceFactory = new CoreTestSupport.RecordingQueryServiceFactory();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var queueManager = new QueueManager(queryServiceFactory, transactionService);

        assertThatThrownBy(() -> queueManager.registerConsumer(ConsumerConfig.<String>builder()
                .queueName(QUEUE_NAME)
                .subscriptionId(SUBSCRIPTION_ID)
                .messageHandler(message -> {
                })
                .properties(ConsumerConfig.Properties.builder()
                        .historyEnabled(true)
                        .build())
                .build()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Call registerQueue(...) first");
    }

    @Test
    void registerQueueFailsOnConflictingRegistration() throws Exception {
        var queryServiceFactory = new CoreTestSupport.RecordingQueryServiceFactory();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var queueManager = new QueueManager(queryServiceFactory, transactionService);

        queueManager.registerQueue(QueueConfig.builder()
                .queueName(QUEUE_NAME)
                .properties(QueueConfig.Properties.builder()
                        .retentionDays(10)
                        .build())
                .build());

        assertThatThrownBy(() -> queueManager.registerQueue(QueueConfig.builder()
                .queueName(QUEUE_NAME)
                .properties(QueueConfig.Properties.builder()
                        .retentionDays(30)
                        .build())
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Queue is already initialized");
    }

    @Test
    void registerConsumerFailsOnConflictingRegistration() throws Exception {
        var queryServiceFactory = new CoreTestSupport.RecordingQueryServiceFactory();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var queueManager = new QueueManager(queryServiceFactory, transactionService);
        queueManager.registerQueue(QueueConfig.builder()
                .queueName(QUEUE_NAME)
                .build());

        queueManager.registerConsumer(ConsumerConfig.<String>builder()
                .queueName(QUEUE_NAME)
                .subscriptionId(SUBSCRIPTION_ID)
                .messageHandler(message -> {
                })
                .build());

        assertThatThrownBy(() -> queueManager.registerConsumer(ConsumerConfig.<String>builder()
                .queueName(QUEUE_NAME)
                .subscriptionId(SUBSCRIPTION_ID)
                .messageHandler(message -> {
                    throw new IllegalStateException();
                })
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Consumer is already registered");
    }
}
