package org.pak.dbq.api;

import lombok.extern.slf4j.Slf4j;
import org.pak.dbq.internal.consumer.ConsumerStarter;
import org.pak.dbq.spi.MessageFactory;
import org.pak.dbq.spi.QueryService;
import org.pak.dbq.spi.TransactionService;
import org.pak.dbq.internal.support.SimpleMessageFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class QueueManager {
    private final QueryService queryService;
    private final TransactionService transactionService;
    private final MessageFactory messageFactory;

    public QueueManager(
            QueryService queryService,
            TransactionService transactionService,
            MessageFactory messageFactory
    ) {
        this.queryService = queryService;
        this.transactionService = transactionService;
        this.messageFactory = messageFactory;
    }

    public QueueManager(
            QueryService queryService,
            TransactionService transactionService
    ) {
        this.queryService = queryService;
        this.transactionService = transactionService;
        this.messageFactory = new SimpleMessageFactory();
    }

    private final ConcurrentHashMap<String, ConsumerStarter<?>> consumerStarters =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConsumerConfig<?>> consumerConfigs =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ProducerConfig<?>> producerConfigs =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Producer<?>> producers =
            new ConcurrentHashMap<>();

    public <T> Producer<T> registerProducer(ProducerConfig<T> producerConfig) {
        var producerKey = producerConfig.getQueueName().name() + "|" + producerConfig.getClazz().getName();
        var existingConfig = producerConfigs.putIfAbsent(producerKey, producerConfig);
        if (existingConfig != null) {
            if (!sameProducerConfig(existingConfig, producerConfig)) {
                throw new IllegalArgumentException(
                        "Producer is already registered for queue %s and class %s with a different config"
                                .formatted(producerConfig.getQueueName(), producerConfig.getClazz().getName()));
            }
            return (Producer<T>) producers.get(producerKey);
        }

        var producer = new Producer<>(producerConfig, queryService, messageFactory);
        producers.put(producerKey, producer);
        return producer;
    }

    public <T> void registerConsumer(ConsumerConfig<T> consumerConfig) {
        var consumerKey = consumerConfig.getQueueName() + "_" + consumerConfig.getSubscriptionId();
        var existingConfig = consumerConfigs.putIfAbsent(consumerKey, consumerConfig);
        if (existingConfig != null) {
            if (!sameConsumerConfig(existingConfig, consumerConfig)) {
                throw new IllegalArgumentException(
                        "Consumer is already registered for queue %s and subscription %s with a different config"
                                .formatted(consumerConfig.getQueueName(), consumerConfig.getSubscriptionId()));
            }
            return;
        }

        consumerStarters.put(consumerKey, new ConsumerStarter<>(consumerConfig, queryService, transactionService,
                messageFactory));
        log.info("Register consumer on queue {} with subscription {}",
                consumerConfig.getQueueName(),
                consumerConfig.getSubscriptionId());
    }

    private boolean sameConsumerConfig(ConsumerConfig<?> left, ConsumerConfig<?> right) {
        return Objects.equals(left.getQueueName(), right.getQueueName())
                && Objects.equals(left.getSubscriptionId(), right.getSubscriptionId())
                && Objects.equals(left.getMessageHandler(), right.getMessageHandler())
                && sameComponent(left.getBlockingPolicy(), right.getBlockingPolicy())
                && sameComponent(left.getRetryablePolicy(), right.getRetryablePolicy())
                && sameComponent(left.getNonRetryablePolicy(), right.getNonRetryablePolicy())
                && sameProperties(left.getProperties(), right.getProperties())
                && sameComponent(left.getMessageContextPropagator(), right.getMessageContextPropagator())
                && sameComponent(left.getMessageConsumerTelemetry(), right.getMessageConsumerTelemetry());
    }

    private boolean sameProperties(ConsumerConfig.Properties left, ConsumerConfig.Properties right) {
        return Objects.equals(left.getMaxPollRecords(), right.getMaxPollRecords())
                && Objects.equals(left.getConcurrency(), right.getConcurrency())
                && Objects.equals(left.getPersistenceExceptionPause(), right.getPersistenceExceptionPause())
                && Objects.equals(left.getUnpredictedExceptionPause(), right.getUnpredictedExceptionPause())
                && left.isHistoryEnabled() == right.isHistoryEnabled()
                && left.getRetentionDays() == right.getRetentionDays();
    }

    private boolean sameProducerConfig(ProducerConfig<?> left, ProducerConfig<?> right) {
        return Objects.equals(left.getQueueName(), right.getQueueName())
                && Objects.equals(left.getClazz(), right.getClazz())
                && sameProducerProperties(left.getProperties(), right.getProperties())
                && sameComponent(left.getMessageContextPropagator(), right.getMessageContextPropagator());
    }

    private boolean sameProducerProperties(ProducerConfig.Properties left, ProducerConfig.Properties right) {
        if (left == right) {
            return true;
        }
        if (left == null || right == null) {
            return false;
        }
        return left.getRetentionDays() == right.getRetentionDays();
    }

    private boolean sameComponent(Object left, Object right) {
        return left == right || Objects.equals(left, right);
    }

    public void startConsumers() {
        consumerStarters.values().forEach(ConsumerStarter::start);
    }

    public void stopConsumers() {
        consumerStarters.values().forEach(ConsumerStarter::stop);
    }
}
