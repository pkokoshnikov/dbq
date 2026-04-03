package org.pak.qdb.runtime;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.experimental.FieldDefaults;
import org.pak.qdb.api.Producer;
import org.pak.qdb.api.ProducerConfig;
import org.pak.qdb.spi.MessageFactory;
import org.pak.qdb.spi.QueryService;

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
