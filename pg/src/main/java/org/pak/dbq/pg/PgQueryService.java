package org.pak.dbq.pg;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.error.DbqException;
import org.pak.dbq.error.NonRetryablePersistenceException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.internal.support.StringFormatter;
import org.pak.dbq.pg.jsonb.JsonbConverter;
import org.pak.dbq.spi.PersistenceService;
import org.postgresql.util.PGobject;

import java.math.BigInteger;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

@Slf4j
public class PgQueryService  {
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
    private final Map<String, Boolean> keyLockTableExistsCache = new ConcurrentHashMap<>();
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

    public void createQueueTable(QueueName queueName) throws DbqException {
        persistenceService.execute(createQueueTableSql(schemaName, queueName));
    }

    public void createSubscriptionTable(QueueName queueName, SubscriptionId subscriptionId, boolean historyEnabled)
            throws DbqException {
        createSubscriptionTable(queueName, subscriptionId, historyEnabled, false);
    }

    public void createSubscriptionTable(
            QueueName queueName,
            SubscriptionId subscriptionId,
            boolean historyEnabled,
            boolean serializedByKey
    ) throws DbqException {
        persistenceService.execute(createSubscriptionTableSql(
                schemaName,
                queueName,
                subscriptionId,
                historyEnabled,
                serializedByKey));
        keyLockTableExistsCache.put(subscriptionId.id(), serializedByKey);
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
        return createSubscriptionTableSql(schemaName, queueName, subscriptionId, historyEnabled, false);
    }

    public static String createSubscriptionTableSql(
            SchemaName schemaName,
            QueueName queueName,
            SubscriptionId subscriptionId,
            boolean historyEnabled,
            boolean serializedByKey
    ) {
        var params = Map.of(
                "schema", schemaName.value(),
                "queueTable", queueTableName(queueName),
                "subscriptionTable", subscriptionTableName(subscriptionId),
                "subscriptionHistoryTable", subscriptionHistoryTableName(subscriptionId),
                "subscriptionKeyLockTable", subscriptionKeyLockTableName(subscriptionId),
                "insertTrigger", subscriptionTableName(subscriptionId) + "_insert_trigger",
                "insertFunction", subscriptionTableName(subscriptionId) + "_insert_function()",
                "subscriptionKeyIndex", subscriptionTableName(subscriptionId) + "_key_idx");

        var sql = new StringBuilder();
        sql.append(STATIC_FORMATTER.execute("""
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
                """, params));

        if (serializedByKey) {
            sql.append('\n').append(STATIC_FORMATTER.execute("""
                    ALTER TABLE ${schema}.${subscriptionTable} ADD COLUMN IF NOT EXISTS key TEXT;

                    CREATE INDEX IF NOT EXISTS ${subscriptionKeyIndex} ON ${schema}.${subscriptionTable}(key);

                    UPDATE ${schema}.${subscriptionTable} s
                    SET key = q.key
                    FROM ${schema}.${queueTable} q
                    WHERE s.key IS NULL
                        AND s.message_id = q.id
                        AND s.originated_at = q.originated_at;

                    CREATE TABLE IF NOT EXISTS ${schema}.${subscriptionKeyLockTable} (
                        key TEXT PRIMARY KEY
                    );

                    INSERT INTO ${schema}.${subscriptionKeyLockTable}(key)
                    SELECT DISTINCT key
                    FROM ${schema}.${subscriptionTable}
                    WHERE key IS NOT NULL
                    ON CONFLICT (key) DO NOTHING;
                    """, params));
        }

        if (historyEnabled) {
            sql.append('\n').append(STATIC_FORMATTER.execute("""
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
                    """, params));
        }

        sql.append('\n').append(STATIC_FORMATTER.execute("""
                CREATE OR REPLACE FUNCTION ${schema}.${insertFunction}
                  RETURNS trigger AS
                $$
                    BEGIN
                    """, params));
        sql.append('\n').append(STATIC_FORMATTER.execute(serializedByKey
                ? """
                    INSERT INTO ${schema}.${subscriptionTable}(message_id, key, created_at, execute_after, originated_at)
                         VALUES(NEW.id, NEW.key, NEW.created_at, NEW.execute_after, NEW.originated_at);

                    INSERT INTO ${schema}.${subscriptionKeyLockTable}(key)
                         VALUES(NEW.key)
                    ON CONFLICT (key) DO NOTHING;
                    """
                : """
                    INSERT INTO ${schema}.${subscriptionTable}(message_id, created_at, execute_after, originated_at)
                         VALUES(NEW.id, NEW.created_at, NEW.execute_after, NEW.originated_at);
                    """,
                params));
        sql.append('\n').append("""
                    RETURN NEW;
                    END;
                $$
                LANGUAGE 'plpgsql';
                """);
        sql.append('\n').append(STATIC_FORMATTER.execute("""
                CREATE OR REPLACE TRIGGER ${insertTrigger}
                    AFTER INSERT ON ${schema}.${queueTable}
                    FOR EACH ROW
                    EXECUTE PROCEDURE ${schema}.${insertFunction};
                """, params));

        return sql.toString();
    }

    public void createPartition(String table, Instant dateTime) throws DbqException {
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

    public DropPartitionResult dropQueuePartition(QueueName queueName, LocalDate partition) throws DbqException {
        var table = queueTable(queueName);
        var partitionName = partitionName(table, partition);
        return dropPartition(table, partitionName, true);
    }

    public DropPartitionResult dropHistoryPartition(SubscriptionId subscriptionId, LocalDate partition)
            throws DbqException {
        var table = subscriptionHistoryTable(subscriptionId);
        var partitionName = partitionName(table, partition);
        return dropPartition(table, partitionName, false);
    }

    public void createQueuePartition(QueueName queueName, Instant includeDateTime) throws DbqException {
        createPartition(queueTable(queueName), includeDateTime);
    }

    public void createHistoryPartition(SubscriptionId subscriptionId, Instant includeDateTime)
            throws DbqException {
        createPartition(subscriptionHistoryTable(subscriptionId), includeDateTime);
    }

    public List<LocalDate> getAllQueuePartitions(QueueName queueName) throws DbqException {
        return getAllPartitions(queueTable(queueName));
    }

    public List<LocalDate> getAllHistoryPartitions(SubscriptionId subscriptionId) throws DbqException {
        return getAllPartitions(subscriptionHistoryTable(subscriptionId));
    }

    private List<LocalDate> getAllPartitions(String tableName) throws DbqException {
        var query = formatter.execute("""
                        SELECT inhrelid::regclass AS partition
                        FROM   pg_catalog.pg_inherits
                        WHERE  inhparent = '${schema}.${table}'::regclass;""",
                Map.of("schema", schemaName.value(), "table", tableName));
        return persistenceService.query(query, rs -> {
            try {
                return LocalDate.parse(rs.getString("partition").replace(tableName + "_", ""), dateFormatter);
            } catch (SQLException e) {
                return sneakyThrow(new NonRetryablePersistenceException(e, e.getCause()));
            }
        });
    }

    @SuppressWarnings("unchecked")
    <T> MessageContainer<T> mapMessageContainer(java.sql.ResultSet rs) {
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
                    jsonbConverter.fromPGobject(rs.getObject("payload", PGobject.class)),
                    jsonbConverter.fromPGHeaders(rs.getObject("headers", PGobject.class)),
                    rs.getString("error_message"),
                    rs.getString("stack_trace"));
        } catch (SQLException e) {
            return sneakyThrow(new NonRetryablePersistenceException(e, e.getCause()));
        } catch (DbqException e) {
            return sneakyThrow(e);
        }
    }

    static String queueTableName(QueueName queueName) {
        return queueName.name().replace("-", "_");
    }

    static String subscriptionTableName(SubscriptionId subscriptionId) {
        return subscriptionId.id().replace("-", "_");
    }

    static String subscriptionHistoryTableName(SubscriptionId subscriptionId) {
        return subscriptionId.id().replace("-", "_") + "_history";
    }

    static String subscriptionKeyLockTableName(SubscriptionId subscriptionId) {
        return subscriptionId.id().replace("-", "_") + "_key_lock";
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

    private String subscriptionKeyLockTable(SubscriptionId subscriptionId) {
        return subscriptionKeyLockTableName(subscriptionId);
    }

    void cleanupKeyLock(SubscriptionId subscriptionId, String key) throws DbqException {
        if (key == null || !hasKeyLockTable(subscriptionId)) {
            return;
        }

        var query = queryCache.computeIfAbsent("cleanupKeyLock|" + subscriptionId.id(), k -> formatter.execute("""
                        DELETE FROM ${schema}.${subscriptionKeyLockTable}
                        WHERE key = ?
                            AND NOT EXISTS (
                                SELECT 1
                                FROM ${schema}.${subscriptionTable}
                                WHERE key = ?
                            )""",
                Map.of("schema", schemaName.value(),
                        "subscriptionTable", subscriptionTable(subscriptionId),
                        "subscriptionKeyLockTable", subscriptionKeyLockTable(subscriptionId))));
        persistenceService.update(query, key, key);
    }

    boolean hasKeyLockTable(SubscriptionId subscriptionId) throws DbqException {
        var cached = keyLockTableExistsCache.get(subscriptionId.id());
        if (cached != null) {
            return cached;
        }

        var query = formatter.execute("""
                SELECT to_regclass('${schema}.${subscriptionKeyLockTable}') IS NOT NULL AS exists
                """, Map.of(
                "schema", schemaName.value(),
                "subscriptionKeyLockTable", subscriptionKeyLockTable(subscriptionId)
        ));

        var result = persistenceService.query(query, rs -> {
            try {
                return rs.getBoolean("exists");
            } catch (SQLException e) {
                return sneakyThrow(new NonRetryablePersistenceException(e, e.getCause()));
            }
        });

        var exists = !result.isEmpty() && Boolean.TRUE.equals(result.get(0));
        var previous = keyLockTableExistsCache.putIfAbsent(subscriptionId.id(), exists);
        return previous != null ? previous : exists;
    }

    private String partitionName(String table, LocalDate partition) {
        return table + "_" + dateFormatter.format(partition);
    }

    void ensureQueuePartitionExists(QueueName queueName, Instant originatedTime) throws DbqException {
        ensurePartitionExists(queueTable(queueName), originatedTime);
    }

    void ensureQueuePartitionsExist(QueueName queueName, List<Instant> originatedTimes) throws DbqException {
        var table = queueTable(queueName);
        var partitionDates = originatedTimes.stream()
                .map(originatedTime -> originatedTime.atOffset(ZoneOffset.UTC).toLocalDate())
                .collect(Collectors.toCollection(LinkedHashSet::new));
        for (var partitionDate : partitionDates) {
            ensurePartitionExists(table, partitionDate.atStartOfDay().toInstant(ZoneOffset.UTC));
        }
    }

    void ensureHistoryPartitionExists(SubscriptionId subscriptionId, Instant originatedTime)
            throws DbqException {
        ensurePartitionExists(subscriptionHistoryTable(subscriptionId), originatedTime);
    }

    private void ensurePartitionExists(String table, Instant originatedTime) throws DbqException {
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

    private void acquirePartitionLock(String partition) throws DbqException {
        persistenceService.execute("SELECT pg_advisory_xact_lock(hashtext(?))", partition);
    }

    private void validatePartitionBounds(String partition, Instant expectedFrom, Instant expectedTo)
            throws DbqException {
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
                return sneakyThrow(new NonRetryablePersistenceException(e, e.getCause()));
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

    private DropPartitionResult dropPartition(String table, String partition, boolean failOnReferences)
            throws DbqException {
        var alreadyAbsent = false;
        try {
            persistenceService.execute(detachPartitionSql(table, partition));
        } catch (DbqException e) {
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
        } catch (DbqException e) {
            if (!isIgnorableMissingPartitionException(e)) {
                throw e;
            }
            alreadyAbsent = true;
            log.debug("Partition {} is already removed, skip drop", partition);
        }

        ensuredPartitions.invalidate(partition);
        return alreadyAbsent ? DropPartitionResult.ALREADY_ABSENT : DropPartitionResult.DROPPED;
    }

    @SuppressWarnings("unchecked")
    private static <T, E extends Throwable> T sneakyThrow(Throwable throwable) throws E {
        throw (E) throwable;
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

    private boolean hasPartitionReferences(DbqException exception) {
        var sqlException = findSqlException(exception);
        return sqlException != null && PARTITION_HAS_REFERENCES_CODE.equals(sqlException.getSQLState());
    }

    private boolean isIgnorableDetachException(DbqException exception) {
        var sqlException = findSqlException(exception);
        if (sqlException == null) {
            return false;
        }

        var message = ofNullable(sqlException.getMessage()).orElse("").toLowerCase(Locale.ROOT);
        return UNDEFINED_TABLE_CODE.equals(sqlException.getSQLState())
                || UNDEFINED_OBJECT_CODE.equals(sqlException.getSQLState())
                || message.contains("is not a partition of relation");
    }

    private boolean isIgnorableMissingPartitionException(DbqException exception) {
        var sqlException = findSqlException(exception);
        if (sqlException == null) {
            return false;
        }

        return UNDEFINED_TABLE_CODE.equals(sqlException.getSQLState())
                || UNDEFINED_OBJECT_CODE.equals(sqlException.getSQLState());
    }

    private SQLException findSqlException(DbqException exception) {
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

    void assertNonEmptyUpdate(int updated, String query) {
        if (updated == 0) {
            log.warn("No records were updated by query '{}'", query);
        }
    }

    PersistenceService persistenceService() {
        return persistenceService;
    }

    SchemaName schemaName() {
        return schemaName;
    }

    JsonbConverter jsonbConverter() {
        return jsonbConverter;
    }

}
