package org.pak.messagebus.core;

import lombok.extern.slf4j.Slf4j;
import org.pak.messagebus.core.service.QueryService;
import org.pak.messagebus.core.service.TransactionService;
import org.slf4j.MDC;

import java.util.List;

import static java.util.Optional.ofNullable;

@Slf4j
class QueueMessagePublisher<T> {
    private final MessageName messageName;
    private final QueryService queryService;
    private final TraceIdExtractor<T> traceIdExtractor;
    private final TransactionService transactionService;

    QueueMessagePublisher(
            PublisherConfig<T> publisherConfig,
            QueryService queryService,
            TransactionService transactionService
    ) {
        this.messageName = publisherConfig.getMessageName();
        this.queryService = queryService;
        this.traceIdExtractor = publisherConfig.getTraceIdExtractor();
        this.transactionService = transactionService;
    }

    public void publish(List<Message<T>> messages) {
        try (var ignoredCollectionMDC = MDC.putCloseable("messageName", messageName.name())) {
            if (log.isDebugEnabled()) {
                messages.forEach(messageDetails -> {
                    var optionalTraceIdMDC = ofNullable(traceIdExtractor.extractTraceId(messageDetails.payload()))
                            .map(v -> MDC.putCloseable("traceId", v));

                    log.debug("Publish payload {}", messageDetails);

                    optionalTraceIdMDC.ifPresent(MDC.MDCCloseable::close);
                });
            }
            var inserted = transactionService.inTransaction(() ->
                    queryService.insertBatchMessage(messageName, messages));
            for (int i = 0; i < inserted.size(); i++) {
                Message<T> message = messages.get(i);
                var optionalTraceIdMDC = ofNullable(traceIdExtractor.extractTraceId(message.payload()))
                        .map(v -> MDC.putCloseable("traceId", v));
                try (var ignoreKeyMDC = MDC.putCloseable("messageKey", message.key())) {
                    if (inserted.get(i)) {
                        log.info("Published payload");
                    } else {
                        log.warn("Duplicate key {}, {}", message.key(), message.originatedTime());
                    }
                } finally {
                    optionalTraceIdMDC.ifPresent(MDC.MDCCloseable::close);
                }
            }
        }
    }
}
