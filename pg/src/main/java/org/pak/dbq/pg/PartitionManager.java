package org.pak.dbq.pg;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.extern.slf4j.Slf4j;
import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.error.DbqException;
import org.pak.dbq.error.NonRetryablePersistenceException;
import org.pak.dbq.internal.support.StringFormatter;
import org.pak.dbq.spi.PersistenceService;

import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public final class PartitionManager {
    private record PartitionBounds(Instant from, Instant to) {
    }

    private static final long ENSURED_PARTITIONS_CACHE_SIZE = 10_000;
    private static final String PARTITION_HAS_REFERENCES_CODE = "23503";
    private static final String UNDEFINED_TABLE_CODE = "42P01";
    private static final String UNDEFINED_OBJECT_CODE = "42704";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy_MM_dd");
    private static final DateTimeFormatter PARTITION_BOUNDARY_FORMATTER = DateTimeFormatter.ISO_INSTANT;
    private static final DateTimeFormatter PARTITION_VALUE_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd['T'][' ']HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
            .optionalEnd()
            .appendOffset("+HH:MM", "Z")
            .toFormatter();
    private static final Pattern PARTITION_BOUNDS_PATTERN =
            Pattern.compile("FOR VALUES FROM \\('([^']+)'\\) TO \\('([^']+)'\\)");
    private final StringFormatter formatter = new StringFormatter();
    private final PersistenceService persistenceService;
    private final Cache<String, Boolean> ensuredPartitions = CacheBuilder.newBuilder()
            .maximumSize(ENSURED_PARTITIONS_CACHE_SIZE)
            .build();
    private final SchemaName schemaName;

    public PartitionManager(SchemaName schemaName, PersistenceService persistenceService) {
        this.schemaName = schemaName;
        this.persistenceService = persistenceService;
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
                "from", PARTITION_BOUNDARY_FORMATTER.format(from),
                "to", PARTITION_BOUNDARY_FORMATTER.format(to)
        ));

        persistenceService.execute(query);
    }

    public DropPartitionResult dropQueuePartition(QueueName queueName, LocalDate partition)
            throws DbqException {
        var table = PgQueryService.queueTableName(queueName);
        var partitionName = partitionName(table, partition);
        return dropPartition(table, partitionName, true);
    }

    public DropPartitionResult dropHistoryPartition(SubscriptionId subscriptionId, LocalDate partition)
            throws DbqException {
        var table = PgQueryService.subscriptionHistoryTableName(subscriptionId);
        var partitionName = partitionName(table, partition);
        return dropPartition(table, partitionName, false);
    }

    public void createQueuePartition(QueueName queueName, Instant includeDateTime) throws DbqException {
        createPartition(PgQueryService.queueTableName(queueName), includeDateTime);
    }

    public void createHistoryPartition(SubscriptionId subscriptionId, Instant includeDateTime)
            throws DbqException {
        createPartition(PgQueryService.subscriptionHistoryTableName(subscriptionId), includeDateTime);
    }

    public List<LocalDate> getAllQueuePartitions(QueueName queueName) throws DbqException {
        return getAllPartitions(PgQueryService.queueTableName(queueName));
    }

    public List<LocalDate> getAllHistoryPartitions(SubscriptionId subscriptionId) throws DbqException {
        return getAllPartitions(PgQueryService.subscriptionHistoryTableName(subscriptionId));
    }

    public void ensureQueuePartitionExists(QueueName queueName, Instant originatedTime) throws DbqException {
        ensurePartitionExists(PgQueryService.queueTableName(queueName), originatedTime);
    }

    public void ensureQueuePartitionsExist(QueueName queueName, List<Instant> originatedTimes) throws DbqException {
        var table = PgQueryService.queueTableName(queueName);
        var partitionDates = originatedTimes.stream()
                .map(originatedTime -> originatedTime.atOffset(ZoneOffset.UTC).toLocalDate())
                .collect(Collectors.toCollection(LinkedHashSet::new));
        for (var partitionDate : partitionDates) {
            ensurePartitionExists(table, partitionDate.atStartOfDay().toInstant(ZoneOffset.UTC));
        }
    }

    public void ensureHistoryPartitionExists(Instant originatedTime, SubscriptionId subscriptionId) throws DbqException {
        ensurePartitionExists(
                PgQueryService.subscriptionHistoryTableName(Objects.requireNonNull(subscriptionId)),
                originatedTime);
    }

    private List<LocalDate> getAllPartitions(String tableName) throws DbqException {
        var query = formatter.execute("""
                        SELECT inhrelid::regclass AS partition
                        FROM   pg_catalog.pg_inherits
                        WHERE  inhparent = '${schema}.${table}'::regclass;""",
                Map.of("schema", schemaName.value(), "table", tableName));
        return persistenceService.query(query, rs -> {
            try {
                return LocalDate.parse(rs.getString("partition").replace(tableName + "_", ""), DATE_FORMATTER);
            } catch (SQLException e) {
                return sneakyThrow(new NonRetryablePersistenceException(e, e.getCause()));
            }
        });
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
        return alreadyAbsent ? DropPartitionResult.ALREADY_ABSENT
                : DropPartitionResult.DROPPED;
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

    private PartitionBounds parsePartitionBounds(String partitionBound) {
        var matcher = PARTITION_BOUNDS_PATTERN.matcher(partitionBound);
        if (!matcher.matches()) {
            throw new IllegalStateException("Unexpected partition bound expression: " + partitionBound);
        }

        return new PartitionBounds(parsePartitionBoundary(matcher.group(1)), parsePartitionBoundary(matcher.group(2)));
    }

    private Instant parsePartitionBoundary(String value) {
        var normalized = value.replace(' ', 'T').replaceAll("([+-]\\d{2})$", "$1:00");
        return OffsetDateTime.parse(normalized, PARTITION_VALUE_FORMATTER).toInstant();
    }

    private String partitionName(String table, LocalDate partition) {
        return table + "_" + DATE_FORMATTER.format(partition);
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

        var message = Objects.requireNonNullElse(sqlException.getMessage(), "").toLowerCase(java.util.Locale.ROOT);
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
        return org.apache.commons.lang3.exception.ExceptionUtils.getThrowableList(exception).stream()
                .filter(SQLException.class::isInstance)
                .map(SQLException.class::cast)
                .findFirst()
                .orElse(null);
    }

    @SuppressWarnings("unchecked")
    private static <T, E extends Throwable> T sneakyThrow(Throwable throwable) throws E {
        throw (E) throwable;
    }
}
