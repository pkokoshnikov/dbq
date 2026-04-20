package org.pak.dbq.pg.consumer;

import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.internal.support.StringFormatter;
import org.pak.dbq.pg.SchemaName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.spi.PersistenceService;

import java.util.List;
import java.util.Map;

public final class DefaultSelectMessagesStrategy implements SelectMessagesStrategy {
    private final PersistenceService persistenceService;
    private final MessageContainerMapper messageContainerMapper;
    private final String query;

    public DefaultSelectMessagesStrategy(
            SchemaName schemaName,
            SubscriptionId subscriptionId,
            String queueTableName,
            Integer maxPollRecords,
            PersistenceService persistenceService,
            MessageContainerMapper messageContainerMapper
    ) {
        this.persistenceService = persistenceService;
        this.messageContainerMapper = messageContainerMapper;
        this.query = new StringFormatter().execute("""
                        SELECT s.id, s.message_id, s.attempt, s.error_message, s.stack_trace, s.created_at, s.updated_at,
                            s.execute_after, m.originated_at, m.key, m.headers, m.payload
                        FROM ${schema}.${subscriptionTable} s JOIN ${schema}.${queueTable} m ON s.message_id = m.id
                            AND s.originated_at = m.originated_at
                        WHERE s.execute_after < CURRENT_TIMESTAMP
                        ORDER BY s.execute_after ASC, s.id ASC
                        LIMIT ${maxPollRecords} FOR UPDATE OF s SKIP LOCKED""",
                Map.of("schema", schemaName.value(),
                        "subscriptionTable", ConsumerTableNames.subscriptionTableName(subscriptionId),
                        "queueTable", queueTableName,
                        "maxPollRecords", maxPollRecords.toString()));
    }

    @Override
    public <T> List<MessageContainer<T>> selectMessages() throws DbqException {
        return persistenceService.query(query, messageContainerMapper::map);
    }
}
