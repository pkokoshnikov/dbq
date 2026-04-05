package org.pak.dbq.pg;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.spi.error.NonRetrayablePersistenceException;
import org.pak.dbq.spi.error.PersistenceException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.api.Message;
import org.pak.dbq.spi.PersistenceService;
import org.pak.dbq.spi.QueryService;
import org.pak.dbq.internal.support.StringFormatter;
import org.pak.dbq.pg.jsonb.JsonbConverter;
import org.postgresql.util.PGobject;

import java.math.BigInteger;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

@Slf4j
public class PgQueryService implements QueryService {
    private static final StringFormatter STATIC_FORMATTER = new StringFormatter();
    private record PartitionBounds(Instant from, Instant to) {
    }

    public enum DropPartitionResult {
        DROPPED,
        ALREADY_ABSENT,
        HAS_REFERENCES
    }

    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy_MM_dd");
    private static final DateTimeFormatter partitionBoundaryFormatter = DateTimeFormatter.ISO_INSTANT;
    private static final DateTimeFormatter partitionValueFormatter = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd['T'][' ']HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
            .optionalEnd()
            .appendOffset("+HH:MM", "Z")
            .toFormatter();
    private static final Pattern partitionBoundsPattern = Pattern.compile("FOR VALUES FROM \\('([^']+)'\\) TO \\('([^']+)'\\)");
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

    public void createQueueTable(QueueName queueName) {
        persistenceService.execute(createQueueTableSql(schemaName, queueName));
    }

    public void createSubscriptionTable(QueueName queueName, SubscriptionId subscriptionId, boolean historyEnabled) {
        persistenceService.execute(createSubscriptionTableSql(schemaName, queueName, subscriptionId, historyEnabled));
    }

    public static String createQueueTableSql(SchemaName schemaName, QueueName queueName) {
        return STATIC_FORMATTER.execute("""
                CREATE TABLE IF NOT EXISTS ${schema}.${queueTable} (
                    id BIGSERIAL,
                    key TEXT,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    execute_after TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    originated_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
                    payload JSONB NOT NULL,
                    PRIMARY KEY (id, originated_at)
                ) PARTITION BY RANGE (originated_at);
                CREATE INDEX IF NOT EXISTS ${queueTable}_created_at_idx ON ${schema}.${queueTable}(created_at);
                CREATE UNIQUE INDEX IF NOT EXISTS ${queueTable}_message_key_idx ON ${schema}.${queueTable}(originated_at, key);
                """, Map.of(
                "schema", schemaName.value(),
                "queueTable", queueTableName(queueName))
        );
    }

    public static String createSubscriptionTableSql(
            SchemaName schemaName,
            QueueName queueName,
            SubscriptionId subscriptionId,
            boolean historyEnabled
    ) {
        var params = Map.of("schema", schemaName.value(),
                "queueTable", queueTableName(queueName),
                "subscriptionTable", subscriptionTableName(subscriptionId),
                "subscriptionHistoryTable", subscriptionHistoryTableName(subscriptionId),
                "insertTrigger", subscriptionTableName(subscriptionId) + "_insert_trigger",
                "insertFunction", subscriptionTableName(subscriptionId) + "_insert_function()");
        var historySql = historyEnabled
                ? STATIC_FORMATTER.execute("""
                        CREATE TABLE IF NOT EXISTS ${schema}.${subscriptionHistoryTable} (
                            id BIGINT,
                            message_id BIGINT NOT NULL,
                            attempt INTEGER NOT NULL DEFAULT 0,
                            status TEXT NOT NULL DEFAULT 'PROCESSED',
                            error_message TEXT,
                            stack_trace TEXT,
                            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            originated_at TIMESTAMP WITH TIME ZONE NOT NULL,
                            FOREIGN KEY (message_id, originated_at) REFERENCES ${schema}.${queueTable}(id, originated_at),
                            PRIMARY KEY (id, originated_at)
                        ) PARTITION BY RANGE (originated_at);

                        CREATE UNIQUE INDEX IF NOT EXISTS ${subscriptionHistoryTable}_message_id_idx ON ${schema}.${subscriptionHistoryTable}(originated_at, message_id);
                        CREATE INDEX IF NOT EXISTS ${subscriptionHistoryTable}_created_at_idx ON ${schema}.${subscriptionHistoryTable}(created_at);
                        """, params)
                : "";

        return STATIC_FORMATTER.execute("""
                        CREATE TABLE IF NOT EXISTS ${schema}.${subscriptionTable} (
                            id BIGSERIAL PRIMARY KEY,
                            message_id BIGINT NOT NULL,
                            attempt INTEGER NOT NULL DEFAULT 0,
                            error_message TEXT,
                            stack_trace TEXT,
                            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP WITH TIME ZONE,
                            originated_at TIMESTAMP WITH TIME ZONE NOT NULL,
                            execute_after TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY (message_id, originated_at) REFERENCES ${schema}.${queueTable}(id, originated_at)
                        );

                        CREATE UNIQUE INDEX IF NOT EXISTS ${subscriptionTable}_message_id_idx ON ${schema}.${subscriptionTable}(message_id);
                        CREATE INDEX IF NOT EXISTS ${subscriptionTable}_created_at_idx ON ${schema}.${subscriptionTable}(created_at);
                        CREATE INDEX IF NOT EXISTS ${subscriptionTable}_execute_after_idx ON ${schema}.${subscriptionTable}(execute_after ASC);

                        ${historySql}

                        CREATE OR REPLACE FUNCTION ${schema}.${insertFunction}
                          RETURNS trigger AS
                        $$
                            BEGIN
                            INSERT INTO ${schema}.${subscriptionTable}(message_id, created_at, execute_after, originated_at)
                                 VALUES(NEW.id, NEW.created_at, NEW.execute_after, NEW.originated_at);
                            RETURN NEW;
                            END;
                        $$
                        LANGUAGE 'plpgsql';

                        CREATE OR REPLACE TRIGGER ${insertTrigger}
                            AFTER INSERT ON ${schema}.${queueTable}
                            FOR EACH ROW
                            EXECUTE PROCEDURE ${schema}.${insertFunction};
                        """,
                Map.of("schema", schemaName.value(),
                        "queueTable", queueTableName(queueName),
                        "subscriptionTable", subscriptionTableName(subscriptionId),
                        "subscriptionHistoryTable", subscriptionHistoryTableName(subscriptionId),
                        "insertTrigger", subscriptionTableName(subscriptionId) + "_insert_trigger",
                        "insertFunction", subscriptionTableName(subscriptionId) + "_insert_function()",
                        "historySql", historySql)
        );
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

    public DropPartitionResult dropQueuePartition(QueueName queueName, LocalDate partition) {
        var table = queueTable(queueName);
        var partitionName = partitionName(table, partition);
        return dropPartition(table, partitionName, true);
    }

    public DropPartitionResult dropHistoryPartition(SubscriptionId subscriptionId, LocalDate partition) {
        var table = subscriptionHistoryTable(subscriptionId);
        var partitionName = partitionName(table, partition);
        return dropPartition(table, partitionName, false);
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

        return persistenceService.insert(query,
                message.key(),
                OffsetDateTime.ofInstant(message.originatedTime(), ZoneId.systemDefault()),
                jsonbConverter.toPGObject(message.headers()),
                jsonbConverter.toPGObject(message.payload())) > 0;
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

        var result = persistenceService.batchInsert(query, args);

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

        var updated = persistenceService.update(query, messageContainer.getId(), e.getMessage(),
                ExceptionUtils.getStackTrace(e));

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

        var updated = persistenceService.update(query, messageContainer.getId());

        assertNonEmptyUpdate(updated, query);
    }

    private static String queueTableName(QueueName queueName) {
        return queueName.name().replace("-", "_");
    }

    private static String subscriptionTableName(SubscriptionId subscriptionId) {
        return subscriptionId.id().replace("-", "_");
    }

    private static String subscriptionHistoryTableName(SubscriptionId subscriptionId) {
        return subscriptionId.id().replace("-", "_") + "_history";
    }

    private String queueTable(QueueName queueName) {
        return queueTableName(queueName);
    }

    private String subscriptionTable(SubscriptionId subscriptionId) {
        return subscriptionTableName(subscriptionId);
    }

    private String subscriptionHistoryTable(SubscriptionId subscriptionId) {
        return subscriptionHistoryTableName(subscriptionId);
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
        var from = partitionDate.atStartOfDay().toInstant(ZoneOffset.UTC);
        var to = partitionDate.plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC);
        if (ensuredPartitions.getIfPresent(partition) != null) {
            return;
        }

        acquirePartitionLock(partition);
        createPartition(table, from);
        validatePartitionBounds(partition, from, to);
        ensuredPartitions.put(partition, Boolean.TRUE);
    }

    private void acquirePartitionLock(String partition) {
        persistenceService.execute("SELECT pg_advisory_xact_lock(hashtext(?))", partition);
    }

    private void validatePartitionBounds(String partition, Instant expectedFrom, Instant expectedTo) {
        var query = formatter.execute("""
                SELECT pg_get_expr(c.relpartbound, c.oid) AS partition_bound
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = '${schema}' AND c.relname = '${partition}'
                """, Map.of("schema", schemaName.value(), "partition", partition));

        var bounds = persistenceService.query(query, rs -> {
            try {
                return parsePartitionBounds(rs.getString("partition_bound"));
            } catch (SQLException e) {
                throw new NonRetrayablePersistenceException(e, e.getCause());
            }
        });

        if (bounds.isEmpty()) {
            throw new IllegalStateException("Ensured partition %s was not found after creation".formatted(partition));
        }

        var actualBounds = bounds.getFirst();
        if (!expectedFrom.equals(actualBounds.from()) || !expectedTo.equals(actualBounds.to())) {
            throw new IllegalStateException(
                    "Unexpected partition bounds for %s. Expected [%s, %s), actual [%s, %s)"
                            .formatted(partition, expectedFrom, expectedTo, actualBounds.from(), actualBounds.to()));
        }
    }

    private DropPartitionResult dropPartition(String table, String partition, boolean failOnReferences) {
        var alreadyAbsent = false;
        try {
            persistenceService.execute(detachPartitionSql(table, partition));
        } catch (PersistenceException e) {
            if (failOnReferences && hasPartitionReferences(e)) {
                return DropPartitionResult.HAS_REFERENCES;
            }
            if (!isIgnorableDetachException(e)) {
                throw e;
            }
            alreadyAbsent = true;
            log.debug("Partition {} is already absent or detached from {}, continue cleanup", partition, table);
        }

        try {
            persistenceService.execute(dropPartitionTableSql(partition));
        } catch (PersistenceException e) {
            if (!isIgnorableMissingPartitionException(e)) {
                throw e;
            }
            alreadyAbsent = true;
            log.debug("Partition {} is already removed, skip drop", partition);
        }

        ensuredPartitions.invalidate(partition);
        return alreadyAbsent ? DropPartitionResult.ALREADY_ABSENT : DropPartitionResult.DROPPED;
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

    private PartitionBounds parsePartitionBounds(String partitionBound) {
        var matcher = partitionBoundsPattern.matcher(partitionBound);
        if (!matcher.matches()) {
            throw new IllegalStateException("Unexpected partition bound expression: " + partitionBound);
        }

        return new PartitionBounds(parsePartitionBoundary(matcher.group(1)), parsePartitionBoundary(matcher.group(2)));
    }

    private Instant parsePartitionBoundary(String value) {
        var normalized = value.replace(' ', 'T').replaceAll("([+-]\\d{2})$", "$1:00");
        return OffsetDateTime.parse(normalized, partitionValueFormatter).toInstant();
    }

    private void assertNonEmptyUpdate(int updated, String query) {
        if (updated == 0) {
            log.warn("No records were updated by query '{}'", query);
        }
    }
}
