package org.pak.dbq.pg.consumer;

import org.pak.dbq.error.DbqException;
import org.pak.dbq.error.NonRetryablePersistenceException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.internal.support.StringFormatter;
import org.pak.dbq.pg.PgQueryService;
import org.pak.dbq.pg.SchemaName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.spi.PersistenceService;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static lombok.Lombok.sneakyThrow;

public final class SerializedByKeySelectMessagesStrategy implements SelectMessagesStrategy {
    private final SchemaName schemaName;
    private final SubscriptionId subscriptionId;
    private final StringFormatter formatter = new StringFormatter();
    private final Map<String, String> queryCache = new ConcurrentHashMap<>();
    private final Map<String, Boolean> hasKeyLockTableCache = new ConcurrentHashMap<>();
    private final PersistenceService persistenceService;

    public SerializedByKeySelectMessagesStrategy(SchemaName schemaName, SubscriptionId subscriptionId, PersistenceService persistenceService) {
        this.schemaName = schemaName;
        this.subscriptionId = subscriptionId;
        this.persistenceService = persistenceService;
    }

    @Override
    public <T> List<MessageContainer<T>> selectMessages(ConsumerQueryContext context) throws DbqException {
        if (!hasKeyLockTable(subscriptionId)) {
            throw new IllegalStateException("Subscription %s was created without serializedByKey support"
                    .formatted(subscriptionId.id()));
        }

        var query = queryCache.computeIfAbsent("selectMessages|" + subscriptionId.id(), k -> formatter.execute("""
                        SELECT s.id, s.message_id, s.attempt, s.error_message, s.stack_trace, s.created_at, s.updated_at,
                            s.execute_after, m.originated_at, s.key, m.headers, m.payload
                        FROM ${schema}.${subscriptionTable} s
                        JOIN ${schema}.${queueTable} m ON s.message_id = m.id
                            AND s.originated_at = m.originated_at
                        JOIN ${schema}.${subscriptionKeyLockTable} k ON k.key = s.key
                        WHERE s.execute_after < CURRENT_TIMESTAMP
                        ORDER BY s.execute_after ASC, s.id ASC
                        LIMIT ${maxPollRecords}
                        FOR UPDATE OF k, s SKIP LOCKED""",
                Map.of("schema", schemaName.value(),
                        "subscriptionTable", PgQueryService.subscriptionTableName(subscriptionId),
                        "queueTable", PgQueryService.queueTableName(context.queueName()),
                        "subscriptionKeyLockTable", PgQueryService.subscriptionKeyLockTableName(subscriptionId),
                        "maxPollRecords", context.maxPollRecords().toString())));

        return context.pgQueryService().persistenceService().query(query, context.pgQueryService()::mapMessageContainer);
    }

    public boolean hasKeyLockTable(SubscriptionId subscriptionId) throws DbqException {
        var cached = hasKeyLockTableCache.get(subscriptionId.id());
        if (cached != null) {
            return cached;
        }

        var query = queryCache.computeIfAbsent("hasKeyLockTable|" + subscriptionId.id(), ignored -> formatter.execute("""
                SELECT to_regclass('${schema}.${subscriptionKeyLockTable}') IS NOT NULL AS exists
                """, Map.of(
                "schema", schemaName.value(),
                "subscriptionKeyLockTable", PgQueryService.subscriptionKeyLockTableName(subscriptionId)
        )));

        var result = persistenceService.query(query, rs -> {
            try {
                return rs.getBoolean("exists");
            } catch (SQLException e) {
                return sneakyThrow(new NonRetryablePersistenceException(e, e.getCause()));
            }
        });

        var exists = !result.isEmpty() && Boolean.TRUE.equals(result.get(0));
        hasKeyLockTableCache.putIfAbsent(subscriptionId.id(), exists);
        return exists;
    }
}
