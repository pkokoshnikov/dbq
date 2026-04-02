package org.pak.messagebus.pg;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.pak.messagebus.core.*;
import org.pak.messagebus.core.error.*;
import org.pak.messagebus.core.service.PersistenceService;
import org.pak.messagebus.core.service.QueryService;
import org.pak.messagebus.pg.jsonb.JsonbConverter;
import org.postgresql.util.PGobject;
import org.postgresql.util.PSQLException;

import java.math.BigInteger;
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

@Slf4j
public class PgQueryService implements QueryService {
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy_MM_dd");
    private static final String MISSING_PARTITION_CODE = "23514";
    private static final String PARTITION_HAS_REFERENCES_CODE = "23503";
    private final PersistenceService persistenceService;
    private final SchemaName schemaName;
    private final JsonbConverter jsonbConverter;
    private final StringFormatter formatter = new StringFormatter();
    private final Map<String, String> queryCache = new ConcurrentHashMap<>();

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
        var query = formatter.execute("""
                CREATE TABLE IF NOT EXISTS ${schema}.${partition}
                PARTITION OF ${schema}.${table} FOR VALUES FROM ('${from}') TO ('${to}');
                """, Map.of(
                "schema", schemaName.value(),
                "table", table,
                "partition", partition,
                "from", dateFormatter.format(date),
                "to", dateFormatter.format(date.plus(1, ChronoUnit.DAYS))
        ));

        persistenceService.execute(query);
    }

    public void dropQueuePartition(QueueName queueName, LocalDate partition) {
        var query = dropPartitionSql(queueTable(queueName), partitionName(queueTable(queueName), partition));
        try {
            persistenceService.execute(query);
        } catch (PersistenceException e) {
            if (PSQLException.class.isAssignableFrom(e.getOriginalCause().getClass()) &&
                    ((PSQLException) e.getOriginalCause()).getSQLState().equals(PARTITION_HAS_REFERENCES_CODE)) {
                throw new PartitionHasReferencesException();
            } else {
                throw e;
            }
        }

    }

    public void dropHistoryPartition(SubscriptionName subscriptionName, LocalDate partition) {
        var query = dropPartitionSql(subscriptionHistoryTable(subscriptionName),
                partitionName(subscriptionHistoryTable(subscriptionName), partition));

        persistenceService.execute(query);
    }

    public void createQueuePartition(QueueName queueName, Instant includeDateTime) {
        createPartition(queueTable(queueName), includeDateTime);
    }

    public void createHistoryPartition(SubscriptionName subscriptionName, Instant includeDateTime) {
        createPartition(subscriptionHistoryTable(subscriptionName), includeDateTime);
    }

    public List<LocalDate> getAllQueuePartitions(QueueName queueName) {
        return getAllPartitions(queueTable(queueName));
    }

    public List<LocalDate> getAllHistoryPartitions(SubscriptionName subscriptionName) {
        return getAllPartitions(subscriptionHistoryTable(subscriptionName));
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
        var query = queryCache.computeIfAbsent("insertMessage|" + queueName.name(), k -> formatter.execute("""
                        INSERT INTO ${schema}.${queueTable} (created_at, execute_after, key, originated_at, payload)
                        VALUES (CURRENT_TIMESTAMP,CURRENT_TIMESTAMP, ?, ?, ?)
                        ON CONFLICT (key, originated_at) DO NOTHING""",
                Map.of("schema", schemaName.value(), "queueTable", queueTable(queueName))));

        return handleMissingPartition(() -> persistenceService.insert(query,
                        message.key(),
                        OffsetDateTime.ofInstant(message.originatedTime(), ZoneId.systemDefault()),
                        jsonbConverter.toPGObject(message.payload())) > 0,
                () -> List.of(message.originatedTime())
        );
    }

    @Override
    public <T> List<Boolean> insertBatchMessage(QueueName queueName, List<Message<T>> messages) {
        var query = queryCache.computeIfAbsent("insertBatchMessage|" + queueName.name(), k -> formatter.execute("""
                        INSERT INTO ${schema}.${queueTable} (created_at, execute_after, key, originated_at, payload)
                        VALUES (CURRENT_TIMESTAMP,CURRENT_TIMESTAMP, ?, ?, ?) ON CONFLICT (key, originated_at) DO NOTHING""",
                Map.of("schema", schemaName.value(), "queueTable", queueTable(queueName))));

        var args = messages.stream()
                .map(t -> new Object[]{t.key(),
                        OffsetDateTime.ofInstant(t.originatedTime(), ZoneId.systemDefault()),
                        jsonbConverter.toPGObject(t.payload())})
                .toList();

        var result = handleMissingPartition(() -> persistenceService.batchInsert(query, args),
                () -> messages.stream().map(Message::originatedTime).collect(Collectors.toList()));

        return Arrays.stream(result).mapToObj(i -> i > 0).toList();
    }

    @Override
    public <T> List<MessageContainer<T>> selectMessages(
            QueueName queueName, SubscriptionName subscriptionName, Integer maxPollRecords
    ) {
        var query = queryCache.computeIfAbsent("selectMessages|" + subscriptionName.name(), k -> formatter.execute("""
                        SELECT s.id, s.message_id, s.attempt, s.error_message, s.stack_trace, s.created_at, s.updated_at,
                            s.execute_after, m.originated_at, m.key, m.payload
                        FROM ${schema}.${subscriptionTable} s JOIN ${schema}.${queueTable} m ON s.message_id = m.id
                            AND s.originated_at = m.originated_at
                        WHERE s.execute_after < CURRENT_TIMESTAMP
                        ORDER BY s.execute_after ASC
                        LIMIT ${maxPollRecords} FOR UPDATE OF s SKIP LOCKED""",
                Map.of("schema", schemaName.value(),
                        "subscriptionTable", subscriptionTable(subscriptionName),
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
                        rs.getString("error_message"), rs.getString("stack_trace"));
            } catch (SQLException e) {
                throw new NonRetrayablePersistenceException(e, e.getCause());
            }
        });
    }

    @Override
    public <T> void retryMessage(
            SubscriptionName subscriptionName, MessageContainer<T> messageContainer, Duration retryDuration, Exception e
    ) {
        var query = queryCache.computeIfAbsent("retryMessage|" + subscriptionName.name(), k -> formatter.execute("""
                        UPDATE ${schema}.${subscriptionTable} SET updated_at = CURRENT_TIMESTAMP,
                            execute_after = CURRENT_TIMESTAMP + (? * INTERVAL '1 second'), attempt = attempt + 1,
                            error_message = ?, stack_trace = ?
                        WHERE id = ?""",
                Map.of("schema", schemaName.value(), "subscriptionTable", subscriptionTable(subscriptionName))));

        var updated = persistenceService.update(query, retryDuration.getSeconds(), e.getMessage(),
                ExceptionUtils.getStackTrace(e),
                messageContainer.getId());

        assertNonEmptyUpdate(updated, query);
    }

    public <T> void failMessage(
            SubscriptionName subscriptionName, MessageContainer<T> messageContainer, Exception e
    ) {
        var query = queryCache.computeIfAbsent("failMessage|" + subscriptionName.name(), k -> formatter.execute("""
                        WITH deleted AS (DELETE FROM ${schema}.${subscriptionTable} WHERE id = ? RETURNING *)
                        INSERT INTO ${schema}.${subscriptionHistoryTable}
                            (id, message_id, originated_at, attempt, status, error_message, stack_trace)
                            SELECT id, message_id, originated_at, attempt, 'FAILED' as status, ?, ? FROM deleted""",
                Map.of("schema", schemaName.value(), "subscriptionTable", subscriptionTable(subscriptionName),
                        "subscriptionHistoryTable", subscriptionHistoryTable(subscriptionName))));

        var updated = handleMissingPartition(
                () -> persistenceService.update(query, messageContainer.getId(), e.getMessage(),
                        ExceptionUtils.getStackTrace(e)), () -> List.of(messageContainer.getOriginatedTime()));

        assertNonEmptyUpdate(updated, query);
    }

    public <T> void completeMessage(
            SubscriptionName subscriptionName, MessageContainer<T> messageContainer
    ) {
        var query = queryCache.computeIfAbsent("completeMessage|" + subscriptionName.name(), k -> formatter.execute("""
                        WITH deleted AS (DELETE FROM ${schema}.${subscriptionTable} WHERE id = ? RETURNING *)
                        INSERT INTO ${schema}.${subscriptionHistoryTable}
                            (id, message_id, originated_at, attempt, status, error_message, stack_trace)
                            SELECT id, message_id, originated_at, attempt, 'PROCESSED' as status, error_message, stack_trace FROM deleted""",
                Map.of("schema", schemaName.value(), "subscriptionTable", subscriptionTable(subscriptionName),
                        "subscriptionHistoryTable", subscriptionHistoryTable(subscriptionName))));

        var updated = handleMissingPartition(
                () -> persistenceService.update(query, messageContainer.getId()),
                () -> List.of(messageContainer.getOriginatedTime()));

        assertNonEmptyUpdate(updated, query);
    }

    private String queueTable(QueueName queueName) {
        return queueName.name().replace("-", "_");
    }

    private String subscriptionTable(SubscriptionName subscriptionName) {
        return subscriptionName.name().replace("-", "_");
    }

    private String subscriptionHistoryTable(SubscriptionName subscriptionName) {
        return subscriptionName.name().replace("-", "_") + "_history";
    }

    private String partitionName(String table, LocalDate partition) {
        return table + "_" + dateFormatter.format(partition);
    }

    private String dropPartitionSql(String table, String partition) {
        return formatter.execute("""
                ALTER TABLE ${schema}.${table} DETACH PARTITION ${schema}.${partition} CONCURRENTLY;
                DROP TABLE IF EXISTS ${schema}.${partition};
                """, Map.of("schema", schemaName.value(), "table", table, "partition", partition));
    }

    private void assertNonEmptyUpdate(int updated, String query) {
        if (updated == 0) {
            log.warn("No records were updated by query '{}'", query);
        }
    }

    private <T> T handleMissingPartition(Supplier<T> operation, Supplier<List<Instant>> originationTimes) {
        try {
            return operation.get();
        } catch (RetrayablePersistenceException e) {
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
