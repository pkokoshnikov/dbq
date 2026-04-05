package org.pak.dbq.api;

import lombok.extern.slf4j.Slf4j;
import org.pak.dbq.internal.consumer.ConsumerStarter;
import org.pak.dbq.spi.MessageFactory;
import org.pak.dbq.spi.QueryService;
import org.pak.dbq.spi.TransactionService;
import org.pak.dbq.internal.support.SimpleMessageFactory;

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
    private final ConcurrentHashMap<Class<?>, Producer<?>> producers =
            new ConcurrentHashMap<>();

    public <T> Producer<T> registerProducer(ProducerConfig<T> producerConfig) {
        return (Producer<T>) producers.computeIfAbsent(producerConfig.getClazz(),
                k -> new Producer<>(producerConfig, queryService, messageFactory));
    }

    public <T> void registerConsumer(ConsumerConfig<T> consumerConfig) {
        consumerStarters.computeIfAbsent(
                consumerConfig.getQueueName() + "_" + consumerConfig.getSubscriptionId(), s -> {
                    var starter = new ConsumerStarter<>(consumerConfig, queryService, transactionService,
                            messageFactory);
                    log.info("Register consumer on queue {} with subscription {}",
                            consumerConfig.getQueueName(),
                            consumerConfig.getSubscriptionId());
                    return starter;
                });
    }

    public void startConsumers() {
        consumerStarters.values().forEach(ConsumerStarter::start);
    }

    public void stopConsumers() {
        consumerStarters.values().forEach(ConsumerStarter::stop);
    }
}
