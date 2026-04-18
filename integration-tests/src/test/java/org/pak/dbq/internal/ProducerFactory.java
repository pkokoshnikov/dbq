package org.pak.dbq.internal;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.experimental.FieldDefaults;
import org.pak.dbq.api.Producer;
import org.pak.dbq.api.ProducerConfig;
import org.pak.dbq.spi.MessageFactory;
import org.pak.dbq.spi.QueryServiceFactory;

@Builder(toBuilder = true)
@FieldDefaults(level = AccessLevel.PRIVATE)
public class ProducerFactory<T> {
    ProducerConfig<T> producerConfig;
    QueryServiceFactory queryServiceFactory;
    MessageFactory messageFactory;

    public Producer<T> create() {
        return new Producer<>(
                producerConfig,
                queryServiceFactory.createProducerQueryService(producerConfig),
                messageFactory);
    }
}
