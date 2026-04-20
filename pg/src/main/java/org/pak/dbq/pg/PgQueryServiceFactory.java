package org.pak.dbq.pg;

import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.ProducerConfig;
import org.pak.dbq.pg.consumer.*;
import org.pak.dbq.pg.producer.PgProducerQueryService;
import org.pak.dbq.spi.ConsumerQueryService;
import org.pak.dbq.spi.ProducerQueryService;
import org.pak.dbq.spi.QueryServiceFactory;

public class PgQueryServiceFactory implements QueryServiceFactory {
    private final QueueTableService pgQueryService;

    public PgQueryServiceFactory(QueueTableService pgQueryService) {
        this.pgQueryService = pgQueryService;
    }

    @Override
    public ProducerQueryService createProducerQueryService(ProducerConfig<?> producerConfig) {
        return new PgProducerQueryService(
                pgQueryService,
                producerConfig.getQueueName(),
                new PartitionService(pgQueryService.schemaName(), pgQueryService.persistenceService()));
    }

    @Override
    public ConsumerQueryService createConsumerQueryService(ConsumerConfig<?> consumerConfig) {
        var properties = consumerConfig.getProperties();
        var schemaName = pgQueryService.schemaName();
        var subscriptionId = consumerConfig.getSubscriptionId();
        var queueTableName = TableNames.queueTableName(consumerConfig.getQueueName());
        var persistenceService = pgQueryService.persistenceService();
        var messageContainerMapper = new MessageContainerMapper(pgQueryService.jsonbConverter());
        var partitionManager = new PartitionService(
                schemaName,
                persistenceService);
        return new PgConsumerQueryService(
                properties.isSerializedByKey()
                        ? new SerializedByKeySelectMessagesStrategy(
                                schemaName,
                                subscriptionId,
                                queueTableName,
                                properties.getMaxPollRecords(),
                                persistenceService,
                                messageContainerMapper)
                        : new DefaultSelectMessagesStrategy(
                                schemaName,
                                subscriptionId,
                                queueTableName,
                                properties.getMaxPollRecords(),
                                persistenceService,
                                messageContainerMapper),
                new DefaultRetryMessageStrategy(schemaName, subscriptionId, persistenceService),
                new CleanupKeyLockFailMessageStrategy(
                        subscriptionId,
                        properties.isHistoryEnabled()
                                ? new HistoryFailMessageStrategy(
                                        schemaName,
                                        subscriptionId,
                                        persistenceService,
                                        partitionManager)
                                : new DefaultFailMessageStrategy(schemaName, subscriptionId, persistenceService),
                        schemaName),
                new CleanupKeyLockCompleteMessageStrategy(
                        subscriptionId,
                        properties.isHistoryEnabled()
                                ? new HistoryCompleteMessageStrategy(
                                        schemaName,
                                        subscriptionId,
                                        persistenceService,
                                        partitionManager)
                                : new DefaultCompleteMessageStrategy(schemaName, subscriptionId, persistenceService),
                        schemaName,
                        persistenceService));
    }
}
