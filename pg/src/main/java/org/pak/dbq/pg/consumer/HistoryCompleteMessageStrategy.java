package org.pak.dbq.pg.consumer;

import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.internal.support.StringFormatter;
import org.pak.dbq.pg.PgQueryService;
import org.pak.dbq.pg.SchemaName;
import org.pak.dbq.api.SubscriptionId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class HistoryCompleteMessageStrategy implements CompleteMessageStrategy {
    private final SchemaName schemaName;
    private final SubscriptionId subscriptionId;
    private final StringFormatter formatter = new StringFormatter();
    private final Map<String, String> queryCache = new ConcurrentHashMap<>();

    public HistoryCompleteMessageStrategy(SchemaName schemaName, SubscriptionId subscriptionId) {
        this.schemaName = schemaName;
        this.subscriptionId = subscriptionId;
    }

    @Override
    public <T> void completeMessage(ConsumerQueryContext context, MessageContainer<T> messageContainer)
            throws DbqException {
        context.pgQueryService().ensureHistoryPartitionExists(subscriptionId, messageContainer.getOriginatedTime());

        var query = queryCache.computeIfAbsent("completeMessage|" + subscriptionId.id(),
                k -> formatter.execute("""
                                WITH deleted AS (
                                    DELETE FROM ${schema}.${subscriptionTable}
                                    WHERE id = ?
                                    RETURNING *
                                )
                                INSERT INTO ${schema}.${subscriptionHistoryTable}
                                    (id, message_id, originated_at, attempt, status, error_message, stack_trace)
                                    SELECT id, message_id, originated_at, attempt, 'PROCESSED' as status, error_message, stack_trace FROM deleted""",
                        Map.of("schema", schemaName.value(),
                                "subscriptionTable", PgQueryService.subscriptionTableName(subscriptionId),
                                "subscriptionHistoryTable", PgQueryService.subscriptionHistoryTableName(subscriptionId))));

        var updated = context.pgQueryService().persistenceService().update(query, messageContainer.getId());

        context.pgQueryService().assertNonEmptyUpdate(updated, query);
    }
}
