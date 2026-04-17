package org.pak.dbq.api;

import lombok.Builder;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.pak.dbq.internal.consumer.ConsumerStarter;
import org.pak.dbq.internal.support.NoOpTableManager;
import org.pak.dbq.internal.support.SimpleMessageFactory;
import org.pak.dbq.spi.MessageFactory;
import org.pak.dbq.spi.QueryService;
import org.pak.dbq.spi.TableManager;
import org.pak.dbq.spi.TransactionService;
import org.pak.dbq.spi.error.PersistenceException;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class QueueManager {
    private final QueryService queryService;
    private final TransactionService transactionService;
    private final MessageFactory messageFactory;
    private final TableManager tableManager;
    private final Properties properties;

    public QueueManager(
            QueryService queryService,
            TransactionService transactionService,
            MessageFactory messageFactory,
            TableManager tableManager,
            Properties properties
    ) {
        this.queryService = queryService;
        this.transactionService = transactionService;
        this.messageFactory = messageFactory;
        this.tableManager = tableManager;
        this.properties = properties != null ? properties : Properties.builder().build();
    }

    public QueueManager(
            QueryService queryService,
            TransactionService transactionService,
            MessageFactory messageFactory,
            TableManager tableManager
    ) {
        this(queryService, transactionService, messageFactory, tableManager, Properties.builder().build());
    }

    public QueueManager(
            QueryService queryService,
            TransactionService transactionService,
            MessageFactory messageFactory
    ) {
        this(queryService, transactionService, messageFactory, new NoOpTableManager(), Properties.builder().build());
    }

    public QueueManager(
            QueryService queryService,
            TransactionService transactionService,
            MessageFactory messageFactory,
            Properties properties
    ) {
        this(queryService, transactionService, messageFactory, new NoOpTableManager(), properties);
    }

    public QueueManager(
            QueryService queryService,
            TransactionService transactionService,
            TableManager tableManager
    ) {
        this(queryService, transactionService, new SimpleMessageFactory(), tableManager, Properties.builder().build());
    }

    public QueueManager(
            QueryService queryService,
            TransactionService transactionService,
            TableManager tableManager,
            Properties properties
    ) {
        this(queryService, transactionService, new SimpleMessageFactory(), tableManager, properties);
    }

    public QueueManager(
            QueryService queryService,
            TransactionService transactionService
    ) {
        this(queryService, transactionService, new SimpleMessageFactory(), new NoOpTableManager(),
                Properties.builder().build());
    }

    private final ConcurrentHashMap<String, ConsumerStarter<?>> consumerStarters =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConsumerConfig<?>> consumerConfigs =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<QueueName, QueueConfig> queueConfigs =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ProducerConfig<?>> producerConfigs =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Producer<?>> producers =
            new ConcurrentHashMap<>();

    public void registerQueue(QueueConfig queueConfig) throws PersistenceException {
        var existingConfig = queueConfigs.putIfAbsent(queueConfig.getQueueName(), queueConfig);
        if (existingConfig != null) {
            if (!sameQueueConfig(existingConfig, queueConfig)) {
                throw new IllegalArgumentException(
                        "Queue is already initialized for %s with a different config"
                                .formatted(queueConfig.getQueueName()));
            }
            return;
        }

        try {
            tableManager.registerQueue(
                    queueConfig.getQueueName(),
                    queueConfig.getProperties().getRetentionDays(),
                    properties.isAutoDdl());
        } catch (PersistenceException | RuntimeException e) {
            queueConfigs.remove(queueConfig.getQueueName(), queueConfig);
            throw e;
        }
    }

    public <T> Producer<T> registerProducer(ProducerConfig<T> producerConfig) {
        requireInitializedQueue(producerConfig.getQueueName());

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

    public <T> void registerConsumer(ConsumerConfig<T> consumerConfig) throws PersistenceException {
        requireInitializedQueue(consumerConfig.getQueueName());

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

        try {
            registerSubscription(consumerConfig);

            consumerStarters.put(consumerKey, new ConsumerStarter<>(consumerConfig, queryService, transactionService,
                    messageFactory));
            log.info("Register consumer on queue {} with subscription {}",
                    consumerConfig.getQueueName(),
                    consumerConfig.getSubscriptionId());
        } catch (PersistenceException | RuntimeException e) {
            consumerConfigs.remove(consumerKey, consumerConfig);
            throw e;
        }
    }

    private boolean sameConsumerConfig(ConsumerConfig<?> left, ConsumerConfig<?> right) {
        return Objects.equals(left.getQueueName(), right.getQueueName())
                && Objects.equals(left.getSubscriptionId(), right.getSubscriptionId())
                && Objects.equals(left.getMessageHandler(), right.getMessageHandler())
                && Objects.equals(left.getBatchMessageHandler(), right.getBatchMessageHandler())
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
                && left.isSerializedByKey() == right.isSerializedByKey();
    }

    private boolean sameProducerConfig(ProducerConfig<?> left, ProducerConfig<?> right) {
        return Objects.equals(left.getQueueName(), right.getQueueName())
                && Objects.equals(left.getClazz(), right.getClazz())
                && sameComponent(left.getMessageContextPropagator(), right.getMessageContextPropagator());
    }

    private boolean sameQueueConfig(QueueConfig left, QueueConfig right) {
        return Objects.equals(left.getQueueName(), right.getQueueName())
                && left.getProperties().getRetentionDays() == right.getProperties().getRetentionDays();
    }

    private void registerSubscription(ConsumerConfig<?> consumerConfig) throws PersistenceException {
        tableManager.registerSubscription(
                consumerConfig.getQueueName(),
                consumerConfig.getSubscriptionId(),
                consumerConfig.getProperties().isHistoryEnabled(),
                consumerConfig.getProperties().isSerializedByKey());
    }

    private void requireInitializedQueue(QueueName queueName) {
        if (!queueConfigs.containsKey(queueName)) {
            throw new IllegalStateException("Queue %s is not registered. Call registerQueue(...) first."
                    .formatted(queueName));
        }
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

    @Builder
    @Getter
    @FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
    public static class Properties {
        boolean autoDdl;
    }
}
