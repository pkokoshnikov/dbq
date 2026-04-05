package org.pak.dbq.pg;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.spi.error.NonRetrayablePersistenceException;
import org.pak.dbq.spi.error.PersistenceException;
import org.pak.dbq.spi.error.RetryablePersistenceException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.api.Message;
import org.pak.dbq.spi.PersistenceService;
import org.pak.dbq.spi.QueryService;
import org.pak.dbq.internal.support.StringFormatter;
import org.pak.dbq.pg.jsonb.JsonbConverter;
import org.postgresql.util.PGobject;
import org.postgresql.util.PSQLException;

import java.math.BigInteger;
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

@Slf4j
public class PgQueryService implements QueryService {
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy_MM_dd");
    private static final DateTimeFormatter partitionBoundaryFormatter = DateTimeFormatter.ISO_INSTANT;
    private static final String MISSING_PARTITION_CODE = "23514";
    private static final String PARTITION_HAS_REFERENCES_CODE = "23503";
    private static final String UNDEFINED_TABLE_CODE = "42P01";
    private static final String UNDEFINED_OBJECT_CODE = "42704";
    private static final long ENSURED_PARTITIONS_CACHE_SIZE = 10_000;
    private final PersistenceService persistenceService;
    private final SchemaName schemaName;
    private final JsonbConverter jsonbConverter;
    private final StringFormatter formatter = new StringFormatter();
    private final Map<String, String> queryCache = new ConcurrentHashMap<>();
    private final Cache<String, Boolean> ensuredPartitions = CacheBuilder.newBuilder()
            .maximumSize(ENSURED_PARTITIONS_CACHE_SIZE)
            .build();

    public PgQueryService(
            PersistenceService persistenceService, SchemaName schemaName, JsonbConverter jsonbConverter
    ) {
        this.persistenceService = persistenceService;
        this.schemaName = schemaName;
        this.jsonbConverter = jsonbConverter;
    }

    public void createPartition(String table, Instant dateTime) {
        var partition = partitionName(table, dateTime.atOffset(ZoneOffset.UTC).toLocalDate());
        log.info("Create partition {}", partition);

        var date = dateTime.atOffset(ZoneOffset.UTC).toLocalDate();
        var from = date.atStartOfDay().toInstant(ZoneOffset.UTC);
        var to = date.plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC);
        var query = formatter.execute("""
                CREATE TABLE IF NOT EXISTS ${schema}.${partition}
                PARTITION OF ${schema}.${table} FOR VALUES FROM ('${from}') TO ('${to}');
                """, Map.of(
                "schema", schemaName.value(),
                "table", table,
                "partition", partition,
                "from", partitionBoundaryFormatter.format(from),
                "to", partitionBoundaryFormatter.format(to)
        ));

        persistenceService.execute(query);
    }

    public void dropQueuePartition(QueueName queueName, LocalDate partition) {
        var table = queueTable(queueName);
        var partitionName = partitionName(table, partition);
        dropPartition(table, partitionName, true);
    }

    public void dropHistoryPartition(SubscriptionId subscriptionId, LocalDate partition) {
        var table = subscriptionHistoryTable(subscriptionId);
        var partitionName = partitionName(table, partition);
        dropPartition(table, partitionName, false);
    }

    public void createQueuePartition(QueueName queueName, Instant includeDateTime) {
        createPartition(queueTable(queueName), includeDateTime);
    }

    public void createHistoryPartition(SubscriptionId subscriptionId, Instant includeDateTime) {
        createPartition(subscriptionHistoryTable(subscriptionId), includeDateTime);
    }

    public List<LocalDate> getAllQueuePartitions(QueueName queueName) {
        return getAllPartitions(queueTable(queueName));
    }

    public List<LocalDate> getAllHistoryPartitions(SubscriptionId subscriptionId) {
        return getAllPartitions(subscriptionHistoryTable(subscriptionId));
    }

    private List<LocalDate> getAllPartitions(String tableName) {
        var query = formatter.execute("""
                        SELECT inhrelid::regclass AS partition
                        FROM   pg_catalog.pg_inherits
                        WHERE  inhparent = '${schema}.${table}'::regclass;""",
                Map.of("schema", schemaName.value(), "table", tableName));
        return persistenceService.query(query, rs -> {
            try {
                return LocalDate.parse(rs.getString("partition").replace(tableName + "_", ""), dateFormatter);
            } catch (SQLException e) {
                throw new NonRetrayablePersistenceException(e, e.getCause());
            }
        });
    }

    @Override
    public <T> boolean insertMessage(QueueName queueName, Message<T> message) {
        ensureQueuePartitionExists(queueName, message.originatedTime());

        var query = queryCache.computeIfAbsent("insertMessage|" + queueName.name(), k -> formatter.execute("""
                        INSERT INTO ${schema}.${queueTable} (created_at, execute_after, key, originated_at, headers, payload)
                        VALUES (CURRENT_TIMESTAMP,CURRENT_TIMESTAMP, ?, ?, ?, ?)
                        ON CONFLICT (key, originated_at) DO NOTHING""",
                Map.of("schema", schemaName.value(), "queueTable", queueTable(queueName))));

        return handleMissingPartition(() -> persistenceService.insert(query,
                        message.key(),
                        OffsetDateTime.ofInstant(message.originatedTime(), ZoneId.systemDefault()),
                        jsonbConverter.toPGObject(message.headers()),
                        jsonbConverter.toPGObject(message.payload())) > 0,
                () -> List.of(message.originatedTime())
        );
    }

    @Override
    public <T> List<Boolean> insertBatchMessage(QueueName queueName, List<Message<T>> messages) {
        ensureQueuePartitionsExist(queueName, messages.stream()
                .map(Message::originatedTime)
                .toList());

        var query = queryCache.computeIfAbsent("insertBatchMessage|" + queueName.name(), k -> formatter.execute("""
                        INSERT INTO ${schema}.${queueTable} (created_at, execute_after, key, originated_at, headers, payload)
                        VALUES (CURRENT_TIMESTAMP,CURRENT_TIMESTAMP, ?, ?, ?, ?) ON CONFLICT (key, originated_at) DO NOTHING""",
                Map.of("schema", schemaName.value(), "queueTable", queueTable(queueName))));

        var args = messages.stream()
                .map(t -> new Object[]{t.key(),
                        OffsetDateTime.ofInstant(t.originatedTime(), ZoneId.systemDefault()),
                        jsonbConverter.toPGObject(t.headers()),
                        jsonbConverter.toPGObject(t.payload())})
                .toList();

        var result = handleMissingPartition(() -> persistenceService.batchInsert(query, args),
                () -> messages.stream().map(Message::originatedTime).collect(Collectors.toList()));

        return Arrays.stream(result).mapToObj(i -> i > 0).toList();
    }

    @Override
    public <T> List<MessageContainer<T>> selectMessages(
            QueueName queueName, SubscriptionId subscriptionId, Integer maxPollRecords
    ) {
        var query = queryCache.computeIfAbsent("selectMessages|" + subscriptionId.id(), k -> formatter.execute("""
                        SELECT s.id, s.message_id, s.attempt, s.error_message, s.stack_trace, s.created_at, s.updated_at,
                            s.execute_after, m.originated_at, m.key, m.headers, m.payload
                        FROM ${schema}.${subscriptionTable} s JOIN ${schema}.${queueTable} m ON s.message_id = m.id
                            AND s.originated_at = m.originated_at
                        WHERE s.execute_after < CURRENT_TIMESTAMP
                        ORDER BY s.execute_after ASC
                        LIMIT ${maxPollRecords} FOR UPDATE OF s SKIP LOCKED""",
                Map.of("schema", schemaName.value(),
                        "subscriptionTable", subscriptionTable(subscriptionId),
                        "queueTable", queueTable(queueName),
                        "maxPollRecords", maxPollRecords.toString())));

        return persistenceService.query(query, rs -> {
            try {
                return new MessageContainer<>(rs.getObject("id", BigInteger.class),
                        rs.getObject("message_id", BigInteger.class),
                        rs.getString("key"),
                        rs.getInt("attempt"),
                        ofNullable(rs.getObject("execute_after", OffsetDateTime.class)).map(OffsetDateTime::toInstant)
                                .orElse(null),
                        ofNullable(rs.getObject("created_at", OffsetDateTime.class)).map(OffsetDateTime::toInstant)
                                .orElse(null),
                        ofNullable(rs.getObject("updated_at", OffsetDateTime.class)).map(OffsetDateTime::toInstant)
                                .orElse(null),
                        ofNullable(rs.getObject("originated_at", OffsetDateTime.class)).map(OffsetDateTime::toInstant)
                                .orElse(null),
                        jsonbConverter.toJsonb(rs.getObject("payload", PGobject.class)),
                        jsonbConverter.toStringMap(rs.getObject("headers", PGobject.class)),
                        rs.getString("error_message"), rs.getString("stack_trace"));
            } catch (SQLException e) {
                throw new NonRetrayablePersistenceException(e, e.getCause());
            }
        });
    }

    @Override
    public <T> void retryMessage(
            SubscriptionId subscriptionId, MessageContainer<T> messageContainer, Duration retryDuration, Exception e
    ) {
        var query = queryCache.computeIfAbsent("retryMessage|" + subscriptionId.id(), k -> formatter.execute("""
                        UPDATE ${schema}.${subscriptionTable} SET updated_at = CURRENT_TIMESTAMP,
                            execute_after = CURRENT_TIMESTAMP + (? * INTERVAL '1 second'), attempt = attempt + 1,
                            error_message = ?, stack_trace = ?
                        WHERE id = ?""",
                Map.of("schema", schemaName.value(), "subscriptionTable", subscriptionTable(subscriptionId))));

        var updated = persistenceService.update(query, retryDuration.getSeconds(), e.getMessage(),
                ExceptionUtils.getStackTrace(e),
                messageContainer.getId());

        assertNonEmptyUpdate(updated, query);
    }

    public <T> void failMessage(
            SubscriptionId subscriptionId,
            MessageContainer<T> messageContainer,
            Exception e,
            boolean historyEnabled
    ) {
        if (!historyEnabled) {
            var query = queryCache.computeIfAbsent("deleteFailedMessage|" + subscriptionId.id(), k -> formatter.execute("""
                            DELETE FROM ${schema}.${subscriptionTable} WHERE id = ?""",
                    Map.of("schema", schemaName.value(), "subscriptionTable", subscriptionTable(subscriptionId))));

            var updated = persistenceService.update(query, messageContainer.getId());
            assertNonEmptyUpdate(updated, query);
            return;
        }

        ensureHistoryPartitionExists(subscriptionId, messageContainer.getOriginatedTime());

        var query = queryCache.computeIfAbsent("failMessage|" + subscriptionId.id(), k -> formatter.execute("""
                        WITH deleted AS (DELETE FROM ${schema}.${subscriptionTable} WHERE id = ? RETURNING *)
                        INSERT INTO ${schema}.${subscriptionHistoryTable}
                            (id, message_id, originated_at, attempt, status, error_message, stack_trace)
                            SELECT id, message_id, originated_at, attempt, 'FAILED' as status, ?, ? FROM deleted""",
                Map.of("schema", schemaName.value(), "subscriptionTable", subscriptionTable(subscriptionId),
                        "subscriptionHistoryTable", subscriptionHistoryTable(subscriptionId))));

        var updated = handleMissingPartition(
                () -> persistenceService.update(query, messageContainer.getId(), e.getMessage(),
                        ExceptionUtils.getStackTrace(e)), () -> List.of(messageContainer.getOriginatedTime()));

        assertNonEmptyUpdate(updated, query);
    }

    public <T> void completeMessage(
            SubscriptionId subscriptionId,
            MessageContainer<T> messageContainer,
            boolean historyEnabled
    ) {
        if (!historyEnabled) {
            var query = queryCache.computeIfAbsent("deleteCompletedMessage|" + subscriptionId.id(), k -> formatter.execute("""
                            DELETE FROM ${schema}.${subscriptionTable} WHERE id = ?""",
                    Map.of("schema", schemaName.value(), "subscriptionTable", subscriptionTable(subscriptionId))));

            var updated = persistenceService.update(query, messageContainer.getId());
            assertNonEmptyUpdate(updated, query);
            return;
        }

        ensureHistoryPartitionExists(subscriptionId, messageContainer.getOriginatedTime());

        var query = queryCache.computeIfAbsent("completeMessage|" + subscriptionId.id(), k -> formatter.execute("""
                        WITH deleted AS (DELETE FROM ${schema}.${subscriptionTable} WHERE id = ? RETURNING *)
                        INSERT INTO ${schema}.${subscriptionHistoryTable}
                            (id, message_id, originated_at, attempt, status, error_message, stack_trace)
                            SELECT id, message_id, originated_at, attempt, 'PROCESSED' as status, error_message, stack_trace FROM deleted""",
                Map.of("schema", schemaName.value(), "subscriptionTable", subscriptionTable(subscriptionId),
                        "subscriptionHistoryTable", subscriptionHistoryTable(subscriptionId))));

        var updated = handleMissingPartition(
                () -> persistenceService.update(query, messageContainer.getId()),
                () -> List.of(messageContainer.getOriginatedTime()));

        assertNonEmptyUpdate(updated, query);
    }

    private String queueTable(QueueName queueName) {
        return queueName.name().replace("-", "_");
    }

    private String subscriptionTable(SubscriptionId subscriptionId) {
        return subscriptionId.id().replace("-", "_");
    }

    private String subscriptionHistoryTable(SubscriptionId subscriptionId) {
        return subscriptionId.id().replace("-", "_") + "_history";
    }

    private String partitionName(String table, LocalDate partition) {
        return table + "_" + dateFormatter.format(partition);
    }

    private void ensureQueuePartitionExists(QueueName queueName, Instant originatedTime) {
        ensurePartitionExists(queueTable(queueName), originatedTime);
    }

    private void ensureQueuePartitionsExist(QueueName queueName, List<Instant> originatedTimes) {
        var table = queueTable(queueName);
        originatedTimes.stream()
                .map(originatedTime -> originatedTime.atOffset(ZoneOffset.UTC).toLocalDate())
                .collect(Collectors.toCollection(LinkedHashSet::new))
                .forEach(partitionDate -> ensurePartitionExists(table, partitionDate.atStartOfDay().toInstant(ZoneOffset.UTC)));
    }

    private void ensureHistoryPartitionExists(SubscriptionId subscriptionId, Instant originatedTime) {
        ensurePartitionExists(subscriptionHistoryTable(subscriptionId), originatedTime);
    }

    private void ensurePartitionExists(String table, Instant originatedTime) {
        var partitionDate = originatedTime.atOffset(ZoneOffset.UTC).toLocalDate();
        var partition = partitionName(table, partitionDate);
        if (ensuredPartitions.getIfPresent(partition) != null) {
            return;
        }

        acquirePartitionLock(partition);
        createPartition(table, partitionDate.atStartOfDay().toInstant(ZoneOffset.UTC));
        ensuredPartitions.put(partition, Boolean.TRUE);
    }

    private void acquirePartitionLock(String partition) {
        persistenceService.execute("SELECT pg_advisory_xact_lock(hashtext(?))", partition);
    }

    private void dropPartition(String table, String partition, boolean failOnReferences) {
        try {
            persistenceService.execute(detachPartitionSql(table, partition));
        } catch (PersistenceException e) {
            if (failOnReferences && hasPartitionReferences(e)) {
                throw new PartitionHasReferencesException(e);
            }
            if (!isIgnorableDetachException(e)) {
                throw e;
            }
            log.debug("Partition {} is already absent or detached from {}, continue cleanup", partition, table);
        }

        try {
            persistenceService.execute(dropPartitionTableSql(partition));
        } catch (PersistenceException e) {
            if (!isIgnorableMissingPartitionException(e)) {
                throw e;
            }
            log.debug("Partition {} is already removed, skip drop", partition);
        }

        ensuredPartitions.invalidate(partition);
    }

    private String detachPartitionSql(String table, String partition) {
        return formatter.execute("""
                ALTER TABLE ${schema}.${table} DETACH PARTITION ${schema}.${partition} CONCURRENTLY;
                """, Map.of("schema", schemaName.value(), "table", table, "partition", partition));
    }

    private String dropPartitionTableSql(String partition) {
        return formatter.execute("""
                DROP TABLE IF EXISTS ${schema}.${partition};
                """, Map.of("schema", schemaName.value(), "partition", partition));
    }

    private boolean hasPartitionReferences(PersistenceException exception) {
        var sqlException = findSqlException(exception);
        return sqlException != null && PARTITION_HAS_REFERENCES_CODE.equals(sqlException.getSQLState());
    }

    private boolean isIgnorableDetachException(PersistenceException exception) {
        var sqlException = findSqlException(exception);
        if (sqlException == null) {
            return false;
        }

        var message = ofNullable(sqlException.getMessage()).orElse("").toLowerCase(Locale.ROOT);
        return UNDEFINED_TABLE_CODE.equals(sqlException.getSQLState())
                || UNDEFINED_OBJECT_CODE.equals(sqlException.getSQLState())
                || message.contains("is not a partition of relation");
    }

    private boolean isIgnorableMissingPartitionException(PersistenceException exception) {
        var sqlException = findSqlException(exception);
        if (sqlException == null) {
            return false;
        }

        return UNDEFINED_TABLE_CODE.equals(sqlException.getSQLState())
                || UNDEFINED_OBJECT_CODE.equals(sqlException.getSQLState());
    }

    private SQLException findSqlException(PersistenceException exception) {
        return ExceptionUtils.getThrowableList(exception).stream()
                .filter(SQLException.class::isInstance)
                .map(SQLException.class::cast)
                .findFirst()
                .orElse(null);
    }

    private void assertNonEmptyUpdate(int updated, String query) {
        if (updated == 0) {
            log.warn("No records were updated by query '{}'", query);
        }
    }

    private <T> T handleMissingPartition(Supplier<T> operation, Supplier<List<Instant>> originationTimes) {
        try {
            return operation.get();
        } catch (RetryablePersistenceException e) {
            if ((e.getOriginalCause().getClass().isAssignableFrom(PSQLException.class))
                    && MISSING_PARTITION_CODE.equals(((SQLException) e.getOriginalCause()).getSQLState())) {
                throw new MissingPartitionException(originationTimes.get());
            } else if (e.getOriginalCause().getClass().isAssignableFrom(BatchUpdateException.class)) {
                for (Throwable throwable : (BatchUpdateException) e.getOriginalCause()) {
                    var sqlException = (SQLException) throwable;
                    if (MISSING_PARTITION_CODE.equals(sqlException.getSQLState())) {
                        throw new MissingPartitionException(originationTimes.get());
                    }
                }
                throw e;
            } else {
                throw e;
            }
        }
    }
}
