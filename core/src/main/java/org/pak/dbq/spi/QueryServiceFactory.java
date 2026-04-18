package org.pak.dbq.spi;

import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.ProducerConfig;

public interface QueryServiceFactory {
    ProducerQueryService createProducerQueryService(ProducerConfig<?> producerConfig);

    ConsumerQueryService createConsumerQueryService(ConsumerConfig<?> consumerConfig);
}
