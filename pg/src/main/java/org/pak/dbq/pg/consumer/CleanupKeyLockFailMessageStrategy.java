package org.pak.dbq.pg.consumer;

import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.internal.support.StringFormatter;
import org.pak.dbq.pg.SchemaName;

import java.util.Map;

public final class CleanupKeyLockFailMessageStrategy implements FailMessageStrategy {
    private final FailMessageStrategy delegate;
    private final String query;

    public CleanupKeyLockFailMessageStrategy(SubscriptionId subscriptionId, FailMessageStrategy delegate, SchemaName schemaName) {
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
    }

    @Override
    public <T> void failMessage(MessageContainer<T> messageContainer, Exception e) throws DbqException {
        delegate.failMessage(messageContainer, e);
    }
}
