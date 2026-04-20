package org.pak.dbq.pg.consumer;

import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.internal.support.StringFormatter;
import org.pak.dbq.pg.PgQueryService;
import org.pak.dbq.pg.SchemaName;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class CleanupKeyLockFailMessageStrategy implements FailMessageStrategy {
    private final SubscriptionId subscriptionId;
    private final FailMessageStrategy delegate;
    private final Map<String, String> queryCache = new ConcurrentHashMap<>();
    private final StringFormatter formatter = new StringFormatter();
    private final SchemaName schemaName;

    public CleanupKeyLockFailMessageStrategy(SubscriptionId subscriptionId, FailMessageStrategy delegate, SchemaName schemaName) {
        this.subscriptionId = subscriptionId;
        this.delegate = delegate;
        this.schemaName = schemaName;
    }

    @Override
    public <T> void failMessage(ConsumerQueryContext context, MessageContainer<T> messageContainer, Exception e)
            throws DbqException {
        delegate.failMessage(context, messageContainer, e);
        queryCache.computeIfAbsent("cleanupKeyLock|" + subscriptionId.id(), ignored -> formatter.execute("""
                        DELETE FROM ${schema}.${subscriptionKeyLockTable}
                        WHERE key = ?
                            AND NOT EXISTS (
                                SELECT 1
                                FROM ${schema}.${subscriptionTable}
                                WHERE key = ?
                            )""",
                Map.of("schema", schemaName.value(),
                        "subscriptionTable", PgQueryService.subscriptionTableName(subscriptionId),
                        "subscriptionKeyLockTable", PgQueryService.subscriptionKeyLockTableName(subscriptionId))));
    }
}
