package org.pak.dbq.pg.consumer;

import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.internal.support.StringFormatter;
import org.pak.dbq.pg.PgQueryService;
import org.pak.dbq.pg.SchemaName;
import org.pak.dbq.spi.PersistenceService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class CleanupKeyLockCompleteMessageStrategy implements CompleteMessageStrategy {
    private final SubscriptionId subscriptionId;
    private final CompleteMessageStrategy delegate;
    private final Map<String, String> queryCache = new ConcurrentHashMap<>();
    private final StringFormatter formatter = new StringFormatter();
    private final SchemaName schemaName;
    private final PersistenceService persistenceServicee;

    public CleanupKeyLockCompleteMessageStrategy(SubscriptionId subscriptionId, CompleteMessageStrategy delegate,
                                                 SchemaName schemaName,
                                                 PersistenceService persistenceServicee) {
        this.subscriptionId = subscriptionId;
        this.delegate = delegate;
        this.schemaName = schemaName;
        this.persistenceServicee = persistenceServicee;
    }

    @Override
    public <T> void completeMessage(ConsumerQueryContext context, MessageContainer<T> messageContainer)
            throws DbqException {
        delegate.completeMessage(context, messageContainer);
        var query = queryCache.computeIfAbsent("cleanupKeyLock|" + subscriptionId.id(), ignored -> formatter.execute("""
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
        persistenceServicee.update(query, messageContainer.getKey(), messageContainer.getKey());
    }
}
