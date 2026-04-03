package org.pak.messagebus.core;

import lombok.extern.slf4j.Slf4j;
import org.pak.messagebus.core.service.QueryService;

import java.time.Instant;
import java.util.UUID;

@Slf4j
public class Producer<T> {
    private final QueueName queueName;
    private final QueryService queryService;
    private final MessageContextPropagator messageContextPropagator;
    private final MessageFactory messageFactory;

    public Producer(
            ProducerConfig<T> producerConfig,
            QueryService queryService,
            MessageFactory messageFactory
    ) {
        this.queueName = producerConfig.getQueueName();
        this.queryService = queryService;
        this.messageContextPropagator = producerConfig.getMessageContextPropagator();
        this.messageFactory = messageFactory;
    }

    public void send(T payload) {
        send(messageFactory.createMessage(UUID.randomUUID().toString(), Instant.now(), payload));
    }

    //TODO: cleaner
    public void send(Message<T> message) {
        var messageToStore = message.withHeaders(messageContextPropagator.injectCurrentContext(message.headers()));

        try (var ignoredCollectionMDC = org.slf4j.MDC.putCloseable("queueName", queueName.name());
                var ignoreKeyMDC = org.slf4j.MDC.putCloseable("messageKey", messageToStore.key())) {
            log.debug("Publish payload {}", messageToStore.payload());

            var inserted = queryService.insertMessage(queueName, messageToStore);
            if (inserted) {
                log.info("Published payload");
            } else {
                log.warn("Duplicate key {}, {}", messageToStore.key(), messageToStore.originatedTime());
            }
        }
    }
}
