package org.pak.messagebus.core;

import lombok.extern.slf4j.Slf4j;
import org.pak.messagebus.core.service.QueryService;
import org.pak.messagebus.core.service.TransactionService;

import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class QueueManager {
    private final QueryService queryService;
    private final TransactionService transactionService;
    private final MessageFactory messageFactory;

    public QueueManager(
            QueryService queryService,
            TransactionService transactionService,
            MessageFactory messageFactory,
            CronConfig cronConfig
    ) {
        this(queryService, transactionService, messageFactory);
    }

    public QueueManager(
            QueryService queryService,
            TransactionService transactionService,
            CronConfig cronConfig
    ) {
        this(queryService, transactionService);
    }

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
        this.messageFactory = new StdMessageFactory();
    }

    private final ConcurrentHashMap<String, QueueProcessorStarter<?>> consumerStarters =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Class<?>, Producer<?>> producers =
            new ConcurrentHashMap<>();

    public <T> void registerProducer(ProducerConfig<T> producerConfig) {
        producers.computeIfAbsent(producerConfig.getClazz(),
                k -> new Producer<>(producerConfig, queryService, messageFactory));
    }

    public <T> void registerConsumer(ConsumerConfig<T> consumerConfig) {
        consumerStarters.computeIfAbsent(
                consumerConfig.getQueueName() + "_" + consumerConfig.getSubscriptionName(), s -> {
                    var starter = new QueueProcessorStarter<>(consumerConfig, queryService, transactionService,
                            messageFactory);
                    log.info("Register consumer on queue {} with subscription {}",
                            consumerConfig.getQueueName().name(),
                            consumerConfig.getSubscriptionName().name());
                    return starter;
                });
    }

    public <T> void publish(T message) {
        ((Producer<T>) producers.get(message.getClass())).publish(message);
    }

    public void startConsumers() {
        consumerStarters.values().forEach(QueueProcessorStarter::start);
    }

    public void stopConsumers() {
        consumerStarters.values().forEach(QueueProcessorStarter::stop);
    }
}
