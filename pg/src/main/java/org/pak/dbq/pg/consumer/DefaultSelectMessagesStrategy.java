package org.pak.dbq.pg.consumer;

import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.internal.support.StringFormatter;
import org.pak.dbq.pg.PgQueryService;
import org.pak.dbq.pg.SchemaName;
import org.pak.dbq.api.SubscriptionId;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class DefaultSelectMessagesStrategy implements SelectMessagesStrategy {
    private final SchemaName schemaName;
    private final SubscriptionId subscriptionId;
    private final StringFormatter formatter = new StringFormatter();
    private final Map<String, String> queryCache = new ConcurrentHashMap<>();

    public DefaultSelectMessagesStrategy(SchemaName schemaName, SubscriptionId subscriptionId) {
        this.schemaName = schemaName;
        this.subscriptionId = subscriptionId;
    }

    @Override
    public <T> List<MessageContainer<T>> selectMessages(ConsumerQueryContext context) throws DbqException {
        var query = queryCache.computeIfAbsent("selectMessages|" + subscriptionId.id(), k -> formatter.execute("""
                        SELECT s.id, s.message_id, s.attempt, s.error_message, s.stack_trace, s.created_at, s.updated_at,
                            s.execute_after, m.originated_at, m.key, m.headers, m.payload
                        FROM ${schema}.${subscriptionTable} s JOIN ${schema}.${queueTable} m ON s.message_id = m.id
                            AND s.originated_at = m.originated_at
                        WHERE s.execute_after < CURRENT_TIMESTAMP
                        ORDER BY s.execute_after ASC, s.id ASC
                        LIMIT ${maxPollRecords} FOR UPDATE OF s SKIP LOCKED""",
                Map.of("schema", schemaName.value(),
                        "subscriptionTable", PgQueryService.subscriptionTableName(subscriptionId),
                        "queueTable", PgQueryService.queueTableName(context.queueName()),
                        "maxPollRecords", context.maxPollRecords().toString())));

        return context.pgQueryService().persistenceService().query(query, context.pgQueryService()::mapMessageContainer);
    }
}
