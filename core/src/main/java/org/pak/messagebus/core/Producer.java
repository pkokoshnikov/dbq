package org.pak.messagebus.core;

import lombok.extern.slf4j.Slf4j;
import org.pak.messagebus.core.service.QueryService;
import org.slf4j.MDC;

import java.time.Instant;
import java.util.UUID;

import static java.util.Optional.ofNullable;

@Slf4j
public class Producer<T> {
    private final QueueName queueName;
    private final QueryService queryService;
    private final TraceIdExtractor<T> traceIdExtractor;
    private final MessageFactory messageFactory;

    public Producer(
            ProducerConfig<T> producerConfig,
            QueryService queryService,
            MessageFactory messageFactory
    ) {
        this.queueName = producerConfig.getQueueName();
        this.queryService = queryService;
        this.traceIdExtractor = producerConfig.getTraceIdExtractor();
        this.messageFactory = messageFactory;
    }

    public void send(T payload) {
        send(messageFactory.createMessage(UUID.randomUUID().toString(), Instant.now(), payload));
    }

    //TODO: cleaner
    public void send(Message<T> message) {
        var optionalTraceIdMDC = ofNullable(traceIdExtractor.extractTraceId(message.payload()))
                .map(v -> MDC.putCloseable("traceId", v));

        try (var ignoredCollectionMDC = MDC.putCloseable("queueName", queueName.name());
                var ignoreKeyMDC = MDC.putCloseable("messageKey", message.key())) {
            log.debug("Publish payload {}", message.payload());

            var inserted = queryService.insertMessage(queueName, message);
            if (inserted) {
                log.info("Published payload");
            } else {
                log.warn("Duplicate key {}, {}", message.key(), message.originatedTime());
            }
        } finally {
            optionalTraceIdMDC.ifPresent(MDC.MDCCloseable::close);
        }
    }
}
