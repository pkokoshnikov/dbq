package org.pak.messagebus.core;

import lombok.extern.slf4j.Slf4j;
import org.pak.messagebus.core.service.QueryService;
import org.slf4j.MDC;

import java.time.Instant;
import java.util.UUID;

import static java.util.Optional.ofNullable;

@Slf4j
class MessagePublisher<T> {
    private final MessageName messageName;
    private final QueryService queryService;
    private final TraceIdExtractor<T> traceIdExtractor;
    private final MessageFactory messageFactory;

    public MessagePublisher(
            PublisherConfig<T> publisherConfig,
            QueryService queryService,
            MessageFactory messageFactory
    ) {
        this.messageName = publisherConfig.getMessageName();
        this.queryService = queryService;
        this.traceIdExtractor = publisherConfig.getTraceIdExtractor();
        this.messageFactory = messageFactory;
    }

    public void publish(T message) {
        publish(messageFactory.createMessage(UUID.randomUUID().toString(), Instant.now(), message));
    }

    //TODO: cleaner
    public void publish(Message<T> message) {
        var optionalTraceIdMDC = ofNullable(traceIdExtractor.extractTraceId(message.payload()))
                .map(v -> MDC.putCloseable("traceId", v));

        try (var ignoredCollectionMDC = MDC.putCloseable("messageName", messageName.name());
                var ignoreKeyMDC = MDC.putCloseable("messageKey", message.key())) {
            log.debug("Publish payload {}", message.payload());

            var inserted = queryService.insertMessage(messageName, message);
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
