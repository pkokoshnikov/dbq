package org.pak.dbq.internal;

import org.junit.jupiter.api.Test;
import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.MessageHandler;
import org.pak.dbq.api.ProducerConfig;
import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.QueueManager;
import org.pak.dbq.api.SubscriptionId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pak.dbq.internal.CoreTestSupport.QUEUE_NAME;

class QueueManagerTest {
    private static final SubscriptionId SUBSCRIPTION_ID = new SubscriptionId("test-subscription");

    @Test
    void registerProducerReturnsProducerThatStoresMessage() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var queue = new QueueManager(queryService, transactionService);

        var producer = queue.registerProducer(ProducerConfig.<String>builder()
                .queueName(QUEUE_NAME)
                .clazz(String.class)
                .properties(ProducerConfig.Properties.builder().retentionDays(10).build())
                .build());

        producer.send("payload");

        assertThat(queryService.getInserts()).hasSize(1);
        assertThat(queryService.getInserts().getFirst().queueName()).isEqualTo(QUEUE_NAME);
        assertThat(queryService.getInserts().getFirst().message().payload()).isEqualTo("payload");
    }

    @Test
    void registerProducerKeepsSeparateProducersForDifferentQueuesWithSamePayloadClass() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var queueManager = new QueueManager(queryService, transactionService);
        var anotherQueue = new QueueName("another-queue");

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

        assertThat(queryService.getInserts()).hasSize(2);
        assertThat(queryService.getInserts().get(0).queueName()).isEqualTo(QUEUE_NAME);
        assertThat(queryService.getInserts().get(1).queueName()).isEqualTo(anotherQueue);
    }

    @Test
    void registerConsumerAllowsIdempotentRegistrationWithSameConfigInstance() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var queueManager = new QueueManager(queryService, transactionService);
        MessageHandler<String> handler = message -> {
        };
        var consumerConfig = ConsumerConfig.<String>builder()
                .queueName(QUEUE_NAME)
                .subscriptionId(SUBSCRIPTION_ID)
                .messageHandler(handler)
                .build();

        queueManager.registerConsumer(consumerConfig);
        queueManager.registerConsumer(consumerConfig);
    }

    @Test
    void registerConsumerFailsOnConflictingRegistration() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var queueManager = new QueueManager(queryService, transactionService);

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
