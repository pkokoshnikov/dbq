package org.pak.dbq.pg;

import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
public class PgSchemaSqlGenerator {
    private final SchemaName schemaName;

    public PgSchemaSqlGenerator(SchemaName schemaName) {
        this.schemaName = schemaName;
    }

    public String createQueueTable(QueueName queueName) {
        return PgQueryService.createQueueTableSql(schemaName, queueName);
    }

    public String createSubscriptionTable(QueueName queueName, SubscriptionId subscriptionId) {
        return createSubscriptionTable(queueName, subscriptionId, false);
    }

    public String createSubscriptionTable(QueueName queueName, SubscriptionId subscriptionId, boolean historyEnabled) {
        return PgQueryService.createSubscriptionTableSql(schemaName, queueName, subscriptionId, historyEnabled);
    }
}
