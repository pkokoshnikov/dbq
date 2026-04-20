package org.pak.dbq.pg;

import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.ProducerConfig;
import org.pak.dbq.spi.ConsumerQueryService;
import org.pak.dbq.spi.ProducerQueryService;
import org.pak.dbq.spi.QueryServiceFactory;

public class PgQueryServiceFactory implements QueryServiceFactory {
    private final PgQueryService pgQueryService;

    public PgQueryServiceFactory(PgQueryService pgQueryService) {
        this.pgQueryService = pgQueryService;
    }

    @Override
    public ProducerQueryService createProducerQueryService(ProducerConfig<?> producerConfig) {
        return new PgProducerQueryService(pgQueryService, producerConfig.getQueueName());
    }

    @Override
    public ConsumerQueryService createConsumerQueryService(ConsumerConfig<?> consumerConfig) {
        var properties = consumerConfig.getProperties();
        if (properties.isSerializedByKey()) {
            return new SerializedByKeyConsumerQueryService(
                    pgQueryService,
                    consumerConfig.getQueueName(),
                    consumerConfig.getSubscriptionId(),
                    properties.getMaxPollRecords(),
                    properties.isHistoryEnabled());
        }
        return new DefaultConsumerQueryService(
                pgQueryService,
                consumerConfig.getQueueName(),
                consumerConfig.getSubscriptionId(),
                properties.getMaxPollRecords(),
                properties.isHistoryEnabled());
    }
}
