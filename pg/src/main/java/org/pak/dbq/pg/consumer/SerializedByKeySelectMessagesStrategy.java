package org.pak.dbq.pg.consumer;

import org.pak.dbq.error.DbqException;
import org.pak.dbq.error.NonRetryablePersistenceException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.internal.support.StringFormatter;
import org.pak.dbq.pg.SchemaName;
import org.pak.dbq.pg.TableNames;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.spi.PersistenceService;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static lombok.Lombok.sneakyThrow;

public final class SerializedByKeySelectMessagesStrategy implements SelectMessagesStrategy {
    private final SubscriptionId subscriptionId;
    private final Map<String, Boolean> hasKeyLockTableCache = new ConcurrentHashMap<>();
    private final PersistenceService persistenceService;
    private final MessageContainerMapper messageContainerMapper;
    private final String selectMessagesQuery;
    private final String hasKeyLockTableQuery;

    public SerializedByKeySelectMessagesStrategy(
            SchemaName schemaName,
            SubscriptionId subscriptionId,
            String queueTableName,
            Integer maxPollRecords,
            PersistenceService persistenceService,
            MessageContainerMapper messageContainerMapper
    ) {
        this.subscriptionId = subscriptionId;
        this.persistenceService = persistenceService;
        this.messageContainerMapper = messageContainerMapper;
        this.selectMessagesQuery = new StringFormatter().execute("""
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
                        "subscriptionTable", TableNames.subscriptionTableName(subscriptionId),
                        "queueTable", queueTableName,
                        "subscriptionKeyLockTable", TableNames.subscriptionKeyLockTableName(subscriptionId),
                        "maxPollRecords", maxPollRecords.toString()));
        this.hasKeyLockTableQuery = new StringFormatter().execute("""
                SELECT to_regclass('${schema}.${subscriptionKeyLockTable}') IS NOT NULL AS exists
                """, Map.of(
                "schema", schemaName.value(),
                "subscriptionKeyLockTable", TableNames.subscriptionKeyLockTableName(subscriptionId)
        ));
    }

    @Override
    public <T> List<MessageContainer<T>> selectMessages() throws DbqException {
        if (!hasKeyLockTable()) {
            throw new IllegalStateException("Subscription %s was created without serializedByKey support"
                    .formatted(subscriptionId.id()));
        }

        return persistenceService.query(selectMessagesQuery, messageContainerMapper::map);
    }

    public boolean hasKeyLockTable() throws DbqException {
        var cached = hasKeyLockTableCache.get(subscriptionId.id());
        if (cached != null) {
            return cached;
        }

        var result = persistenceService.query(hasKeyLockTableQuery, rs -> {
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
