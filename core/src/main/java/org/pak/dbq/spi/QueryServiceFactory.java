package org.pak.dbq.spi;

import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.ProducerConfig;

public interface QueryServiceFactory {
    QueryService createProducerQueryService(ProducerConfig<?> producerConfig);

    QueryService createConsumerQueryService(ConsumerConfig<?> consumerConfig);
}
