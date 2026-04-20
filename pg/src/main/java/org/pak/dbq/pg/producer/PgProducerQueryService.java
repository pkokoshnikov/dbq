package org.pak.dbq.pg.producer;

import org.pak.dbq.api.Message;
import org.pak.dbq.api.QueueName;
import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.support.StringFormatter;
import org.pak.dbq.pg.PgQueryService;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class PgProducerQueryService implements org.pak.dbq.spi.ProducerQueryService {
    private final PgQueryService pgQueryService;
    private final QueueName queueName;
    private final StringFormatter formatter = new StringFormatter();
    private final Map<String, String> queryCache = new ConcurrentHashMap<>();

    public PgProducerQueryService(PgQueryService pgQueryService, QueueName queueName) {
        this.pgQueryService = pgQueryService;
        this.queueName = queueName;
    }

    @Override
    public <T> boolean insertMessage(Message<T> message) throws DbqException {
        pgQueryService.ensureQueuePartitionExists(queueName, message.originatedTime());

        var query = queryCache.computeIfAbsent("insertMessage", k -> formatter.execute("""
                        INSERT INTO ${schema}.${queueTable} (created_at, execute_after, key, originated_at, headers, payload)
                        VALUES (CURRENT_TIMESTAMP,CURRENT_TIMESTAMP, ?, ?, ?, ?)
                        ON CONFLICT (key, originated_at) DO NOTHING""",
                Map.of("schema", pgQueryService.schemaName().value(),
                        "queueTable", PgQueryService.queueTableName(queueName))));

        return pgQueryService.persistenceService().insert(query,
                message.key(),
                OffsetDateTime.ofInstant(message.originatedTime(), ZoneId.systemDefault()),
                pgQueryService.jsonbConverter().toPGObject(message.headers()),
                pgQueryService.jsonbConverter().toPGObject(message.payload())) > 0;
    }

    @Override
    public <T> List<Boolean> insertBatchMessage(List<Message<T>> messages) throws DbqException {
        pgQueryService.ensureQueuePartitionsExist(queueName, messages.stream()
                .map(Message::originatedTime)
                .toList());

        var query = queryCache.computeIfAbsent("insertBatchMessage", k -> formatter.execute("""
                        INSERT INTO ${schema}.${queueTable} (created_at, execute_after, key, originated_at, headers, payload)
                        VALUES (CURRENT_TIMESTAMP,CURRENT_TIMESTAMP, ?, ?, ?, ?) ON CONFLICT (key, originated_at) DO NOTHING""",
                Map.of("schema", pgQueryService.schemaName().value(),
                        "queueTable", PgQueryService.queueTableName(queueName))));

        var args = new java.util.ArrayList<Object[]>(messages.size());
        for (var message : messages) {
            args.add(new Object[]{
                    message.key(),
                    OffsetDateTime.ofInstant(message.originatedTime(), ZoneId.systemDefault()),
                    pgQueryService.jsonbConverter().toPGObject(message.headers()),
                    pgQueryService.jsonbConverter().toPGObject(message.payload())
            });
        }

        var result = pgQueryService.persistenceService().batchInsert(query, args);

        return Arrays.stream(result).mapToObj(i -> i > 0).toList();
    }
}
