package org.pak.dbq.pg.consumer;

import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.internal.support.StringFormatter;
import org.pak.dbq.pg.PgQueryService;
import org.pak.dbq.pg.SchemaName;
import org.pak.dbq.api.SubscriptionId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class DefaultFailMessageStrategy implements FailMessageStrategy {
    private final SchemaName schemaName;
    private final SubscriptionId subscriptionId;
    private final StringFormatter formatter = new StringFormatter();
    private final Map<String, String> queryCache = new ConcurrentHashMap<>();

    public DefaultFailMessageStrategy(SchemaName schemaName, SubscriptionId subscriptionId) {
        this.schemaName = schemaName;
        this.subscriptionId = subscriptionId;
    }

    @Override
    public <T> void failMessage(ConsumerQueryContext context, MessageContainer<T> messageContainer, Exception e)
            throws DbqException {
        var query = queryCache.computeIfAbsent("deleteFailedMessage|" + subscriptionId.id(),
                k -> formatter.execute("""
                                DELETE FROM ${schema}.${subscriptionTable} WHERE id = ?""",
                        Map.of("schema", schemaName.value(),
                                "subscriptionTable", PgQueryService.subscriptionTableName(subscriptionId))));

        var updated = context.pgQueryService().persistenceService().update(query, messageContainer.getId());
        context.pgQueryService().assertNonEmptyUpdate(updated, query);
    }
}
