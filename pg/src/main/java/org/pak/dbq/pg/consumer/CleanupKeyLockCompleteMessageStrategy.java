package org.pak.dbq.pg.consumer;

import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.internal.support.StringFormatter;
import org.pak.dbq.pg.SchemaName;
import org.pak.dbq.spi.PersistenceService;

import java.util.Map;

public final class CleanupKeyLockCompleteMessageStrategy implements CompleteMessageStrategy {
    private final CompleteMessageStrategy delegate;
    private final PersistenceService persistenceServicee;
    private final String query;

    public CleanupKeyLockCompleteMessageStrategy(SubscriptionId subscriptionId, CompleteMessageStrategy delegate,
                                                 SchemaName schemaName,
                                                 PersistenceService persistenceServicee) {
        this.delegate = delegate;
        this.query = new StringFormatter().execute("""
                        DELETE FROM ${schema}.${subscriptionKeyLockTable}
                        WHERE key = ?
                            AND NOT EXISTS (
                                SELECT 1
                                FROM ${schema}.${subscriptionTable}
                                WHERE key = ?
                            )""",
                Map.of("schema", schemaName.value(),
                        "subscriptionTable", ConsumerTableNames.subscriptionTableName(subscriptionId),
                        "subscriptionKeyLockTable", ConsumerTableNames.subscriptionKeyLockTableName(subscriptionId)));
        this.persistenceServicee = persistenceServicee;
    }

    @Override
    public <T> void completeMessage(MessageContainer<T> messageContainer) throws DbqException {
        delegate.completeMessage(messageContainer);
        persistenceServicee.update(query, messageContainer.getKey(), messageContainer.getKey());
    }
}
