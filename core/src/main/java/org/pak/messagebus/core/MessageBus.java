package org.pak.messagebus.core;

import lombok.extern.slf4j.Slf4j;
import org.pak.messagebus.core.service.QueryService;
import org.pak.messagebus.core.service.TransactionService;

import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MessageBus {
    private final QueryService queryService;
    private final TransactionService transactionService;
    private final MessageFactory messageFactory;

    public MessageBus(
            QueryService queryService,
            TransactionService transactionService,
            MessageFactory messageFactory,
            CronConfig cronConfig
    ) {
        this(queryService, transactionService, messageFactory);
    }

    public MessageBus(
            QueryService queryService,
            TransactionService transactionService,
            CronConfig cronConfig
    ) {
        this(queryService, transactionService);
    }

    public MessageBus(
            QueryService queryService,
            TransactionService transactionService,
            MessageFactory messageFactory
    ) {
        this.queryService = queryService;
        this.transactionService = transactionService;
        this.messageFactory = messageFactory;
    }

    public MessageBus(
            QueryService queryService,
            TransactionService transactionService
    ) {
        this.queryService = queryService;
        this.transactionService = transactionService;
        this.messageFactory = new StdMessageFactory();
    }

    private final ConcurrentHashMap<String, MessageProcessorStarter<?>> messageProcessorStarters =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Class<?>, MessagePublisher<?>> messagePublishers =
            new ConcurrentHashMap<>();

    public <T> void registerPublisher(PublisherConfig<T> publisherConfig) {
        messagePublishers.computeIfAbsent(publisherConfig.getClazz(),
                k -> new MessagePublisher<>(publisherConfig, queryService, messageFactory));
    }

    public <T> void registerSubscriber(SubscriberConfig<T> subscriberConfig) {
        messageProcessorStarters.computeIfAbsent(
                subscriberConfig.getMessageName() + "_" + subscriberConfig.getSubscriptionName(), s -> {
                    var starter = new MessageProcessorStarter<>(subscriberConfig, queryService, transactionService,
                            messageFactory);
                    log.info("Register subscriber on payload {} with subscription {}",
                            subscriberConfig.getMessageName(),
                            subscriberConfig.getSubscriptionName());
                    return starter;
                });
    }

    public <T> void publish(T message) {
        ((MessagePublisher<T>) messagePublishers.get(message.getClass())).publish(message);
    }

    public void startSubscribers() {
        messageProcessorStarters.values().forEach(MessageProcessorStarter::start);
    }

    public void stopSubscribers() {
        messageProcessorStarters.values().forEach(MessageProcessorStarter::stop);
    }
}
