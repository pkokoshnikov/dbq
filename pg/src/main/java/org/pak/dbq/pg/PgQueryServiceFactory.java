package org.pak.dbq.pg;

import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.ProducerConfig;
import org.pak.dbq.pg.consumer.*;
import org.pak.dbq.pg.producer.PgProducerQueryService;
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
        var schemaName = pgQueryService.schemaName();
        var subscriptionId = consumerConfig.getSubscriptionId();
        var context = new ConsumerQueryContext(
                pgQueryService,
                consumerConfig.getQueueName(),
                subscriptionId,
                properties.getMaxPollRecords(),
                properties.isHistoryEnabled());
        return new PgConsumerQueryService(
                context,
                properties.isSerializedByKey()
                        ? new SerializedByKeySelectMessagesStrategy(schemaName, subscriptionId)
                        : new DefaultSelectMessagesStrategy(schemaName, subscriptionId),
                new DefaultRetryMessageStrategy(schemaName, subscriptionId),
                new CleanupKeyLockFailMessageStrategy(
                        subscriptionId,
                        properties.isHistoryEnabled()
                                ? new HistoryFailMessageStrategy(schemaName, subscriptionId)
                                : new DefaultFailMessageStrategy(schemaName, subscriptionId),
                        schemaName),
                new CleanupKeyLockCompleteMessageStrategy(
                        subscriptionId,
                        properties.isHistoryEnabled()
                                ? new HistoryCompleteMessageStrategy(schemaName, subscriptionId)
                                : new DefaultCompleteMessageStrategy(schemaName, subscriptionId),
                        schemaName));
    }
}
