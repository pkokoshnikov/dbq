package org.pak.messagebus.core;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.experimental.FieldDefaults;
import org.pak.messagebus.core.service.QueryService;
import org.pak.messagebus.core.service.TransactionService;

@Builder(toBuilder = true)
@FieldDefaults(level = AccessLevel.PRIVATE)
class QueueMessagePublisherFactory<T> {
    PublisherConfig<T> publisherConfig;
    QueryService queryService;
    MessageFactory messageFactory;
    TransactionService transactionService;

    QueueMessagePublisher<T> create() {
        return new QueueMessagePublisher<>(publisherConfig, queryService, transactionService);
    }
}
