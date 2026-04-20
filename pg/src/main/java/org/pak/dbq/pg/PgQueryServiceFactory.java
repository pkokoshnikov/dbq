package org.pak.dbq.pg;

import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.ProducerConfig;
import org.pak.dbq.pg.consumer.*;
import org.pak.dbq.pg.jsonb.JsonbConverter;
import org.pak.dbq.pg.producer.PgProducerQueryService;
import org.pak.dbq.spi.PersistenceService;
import org.pak.dbq.spi.ConsumerQueryService;
import org.pak.dbq.spi.ProducerQueryService;
import org.pak.dbq.spi.QueryServiceFactory;

public class PgQueryServiceFactory implements QueryServiceFactory {
    private final QueueTableService queueTableService;
    private final PersistenceService persistenceService;
    private final SchemaName schemaName;
    private final JsonbConverter jsonbConverter;

    public PgQueryServiceFactory(
            QueueTableService queueTableService,
            PersistenceService persistenceService,
            SchemaName schemaName,
            JsonbConverter jsonbConverter
    ) {
        this.queueTableService = queueTableService;
        this.persistenceService = persistenceService;
        this.schemaName = schemaName;
        this.jsonbConverter = jsonbConverter;
    }

    @Override
    public ProducerQueryService createProducerQueryService(ProducerConfig<?> producerConfig) {
        return new PgProducerQueryService(
                persistenceService,
                schemaName,
                jsonbConverter,
                producerConfig.getQueueName(),
                new QueuePartitionService(schemaName, persistenceService));
    }

    @Override
    public ConsumerQueryService createConsumerQueryService(ConsumerConfig<?> consumerConfig) {
        var properties = consumerConfig.getProperties();
        var subscriptionId = consumerConfig.getSubscriptionId();
        var queueTableName = TableNames.queueTableName(consumerConfig.getQueueName());
        var messageContainerMapper = new MessageContainerMapper(jsonbConverter);
        var partitionManager = new QueuePartitionService(
                schemaName,
                persistenceService);
        var failMessageStrategy = properties.isHistoryEnabled()
                ? new HistoryFailMessageStrategy(
                        schemaName,
                        subscriptionId,
                        persistenceService,
                        partitionManager)
                : new DefaultFailMessageStrategy(schemaName, subscriptionId, persistenceService);
        if (properties.isSerializedByKey()) {
            failMessageStrategy = new CleanupKeyLockFailMessageStrategy(
                    subscriptionId,
                    failMessageStrategy,
                    schemaName,
                    persistenceService);
        }

        var completeMessageStrategy = properties.isHistoryEnabled()
                ? new HistoryCompleteMessageStrategy(
                        schemaName,
                        subscriptionId,
                        persistenceService,
                        partitionManager)
                : new DefaultCompleteMessageStrategy(schemaName, subscriptionId, persistenceService);
        if (properties.isSerializedByKey()) {
            completeMessageStrategy = new CleanupKeyLockCompleteMessageStrategy(
                    subscriptionId,
                    completeMessageStrategy,
                    schemaName,
                    persistenceService);
        }

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
                failMessageStrategy,
                completeMessageStrategy);
    }
}
