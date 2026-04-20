package org.pak.dbq.pg;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.internal.support.StringFormatter;
import org.pak.dbq.spi.ConsumerQueryService;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class DefaultConsumerQueryService implements ConsumerQueryService {
    private final PgQueryService pgQueryService;
    private final QueueName queueName;
    private final SubscriptionId subscriptionId;
    private final Integer maxPollRecords;
    private final boolean historyEnabled;
    private final StringFormatter formatter = new StringFormatter();
    private final Map<String, String> queryCache = new ConcurrentHashMap<>();

    public DefaultConsumerQueryService(
            PgQueryService pgQueryService,
            QueueName queueName,
            SubscriptionId subscriptionId,
            Integer maxPollRecords,
            boolean historyEnabled
    ) {
        this.pgQueryService = pgQueryService;
        this.queueName = queueName;
        this.subscriptionId = subscriptionId;
        this.maxPollRecords = maxPollRecords;
        this.historyEnabled = historyEnabled;
    }

    @Override
    public <T> List<MessageContainer<T>> selectMessages() throws DbqException {
        var query = queryCache.computeIfAbsent("selectMessages", k -> formatter.execute("""
                        SELECT s.id, s.message_id, s.attempt, s.error_message, s.stack_trace, s.created_at, s.updated_at,
                            s.execute_after, m.originated_at, m.key, m.headers, m.payload
                        FROM ${schema}.${subscriptionTable} s JOIN ${schema}.${queueTable} m ON s.message_id = m.id
                            AND s.originated_at = m.originated_at
                        WHERE s.execute_after < CURRENT_TIMESTAMP
                        ORDER BY s.execute_after ASC, s.id ASC
                        LIMIT ${maxPollRecords} FOR UPDATE OF s SKIP LOCKED""",
                Map.of("schema", pgQueryService.schemaName().value(),
                        "subscriptionTable", PgQueryService.subscriptionTableName(subscriptionId),
                        "queueTable", PgQueryService.queueTableName(queueName),
                        "maxPollRecords", maxPollRecords.toString())));

        return pgQueryService.persistenceService().query(query, pgQueryService::mapMessageContainer);
    }

    @Override
    public <T> void retryMessage(MessageContainer<T> messageContainer, Duration retryDuration, Exception e)
            throws DbqException {
        var query = queryCache.computeIfAbsent("retryMessage", k -> formatter.execute("""
                        UPDATE ${schema}.${subscriptionTable} SET updated_at = CURRENT_TIMESTAMP,
                            execute_after = CURRENT_TIMESTAMP + (? * INTERVAL '1 second'), attempt = attempt + 1,
                            error_message = ?, stack_trace = ?
                        WHERE id = ?""",
                Map.of("schema", pgQueryService.schemaName().value(),
                        "subscriptionTable", PgQueryService.subscriptionTableName(subscriptionId))));

        var updated = pgQueryService.persistenceService().update(query, retryDuration.getSeconds(), e.getMessage(),
                ExceptionUtils.getStackTrace(e),
                messageContainer.getId());

        pgQueryService.assertNonEmptyUpdate(updated, query);
    }

    @Override
    public <T> void failMessage(MessageContainer<T> messageContainer, Exception e) throws DbqException {
        if (!historyEnabled) {
            var query = queryCache.computeIfAbsent("deleteFailedMessage", k -> formatter.execute("""
                            DELETE FROM ${schema}.${subscriptionTable} WHERE id = ?""",
                    Map.of("schema", pgQueryService.schemaName().value(),
                            "subscriptionTable", PgQueryService.subscriptionTableName(subscriptionId))));

            var updated = pgQueryService.persistenceService().update(query, messageContainer.getId());
            pgQueryService.assertNonEmptyUpdate(updated, query);
            pgQueryService.cleanupKeyLock(subscriptionId, messageContainer.getKey());
            return;
        }

        pgQueryService.ensureHistoryPartitionExists(subscriptionId, messageContainer.getOriginatedTime());

        var query = queryCache.computeIfAbsent("failMessage", k -> formatter.execute("""
                        WITH deleted AS (
                            DELETE FROM ${schema}.${subscriptionTable}
                            WHERE id = ?
                            RETURNING *
                        )
                        INSERT INTO ${schema}.${subscriptionHistoryTable}
                            (id, message_id, originated_at, attempt, status, error_message, stack_trace)
                            SELECT id, message_id, originated_at, attempt, 'FAILED' as status, ?, ? FROM deleted""",
                Map.of("schema", pgQueryService.schemaName().value(),
                        "subscriptionTable", PgQueryService.subscriptionTableName(subscriptionId),
                        "subscriptionHistoryTable", PgQueryService.subscriptionHistoryTableName(subscriptionId))));

        var updated = pgQueryService.persistenceService().update(query, messageContainer.getId(), e.getMessage(),
                ExceptionUtils.getStackTrace(e));

        pgQueryService.assertNonEmptyUpdate(updated, query);
        pgQueryService.cleanupKeyLock(subscriptionId, messageContainer.getKey());
    }

    @Override
    public <T> void completeMessage(MessageContainer<T> messageContainer) throws DbqException {
        if (!historyEnabled) {
            var query = queryCache.computeIfAbsent("deleteCompletedMessage", k -> formatter.execute("""
                            DELETE FROM ${schema}.${subscriptionTable} WHERE id = ?""",
                    Map.of("schema", pgQueryService.schemaName().value(),
                            "subscriptionTable", PgQueryService.subscriptionTableName(subscriptionId))));

            var updated = pgQueryService.persistenceService().update(query, messageContainer.getId());
            pgQueryService.assertNonEmptyUpdate(updated, query);
            pgQueryService.cleanupKeyLock(subscriptionId, messageContainer.getKey());
            return;
        }

        pgQueryService.ensureHistoryPartitionExists(subscriptionId, messageContainer.getOriginatedTime());

        var query = queryCache.computeIfAbsent("completeMessage", k -> formatter.execute("""
                        WITH deleted AS (
                            DELETE FROM ${schema}.${subscriptionTable}
                            WHERE id = ?
                            RETURNING *
                        )
                        INSERT INTO ${schema}.${subscriptionHistoryTable}
                            (id, message_id, originated_at, attempt, status, error_message, stack_trace)
                            SELECT id, message_id, originated_at, attempt, 'PROCESSED' as status, error_message, stack_trace FROM deleted""",
                Map.of("schema", pgQueryService.schemaName().value(),
                        "subscriptionTable", PgQueryService.subscriptionTableName(subscriptionId),
                        "subscriptionHistoryTable", PgQueryService.subscriptionHistoryTableName(subscriptionId))));

        var updated = pgQueryService.persistenceService().update(query, messageContainer.getId());

        pgQueryService.assertNonEmptyUpdate(updated, query);
        pgQueryService.cleanupKeyLock(subscriptionId, messageContainer.getKey());
    }
}
