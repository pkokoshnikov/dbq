package org.pak.messagebus.core;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.experimental.FieldDefaults;
import org.pak.messagebus.core.service.QueryService;

@Builder(toBuilder = true)
@FieldDefaults(level = AccessLevel.PRIVATE)
class ProducerFactory<T> {
    ProducerConfig<T> producerConfig;
    QueryService queryService;
    MessageFactory messageFactory;

    Producer<T> create() {
        return new Producer<>(producerConfig, queryService, messageFactory);
    }
}
