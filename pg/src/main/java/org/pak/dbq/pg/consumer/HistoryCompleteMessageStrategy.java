package org.pak.dbq.pg.consumer;

import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.internal.support.StringFormatter;
import org.pak.dbq.pg.PartitionManager;
import org.pak.dbq.pg.SchemaName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.spi.PersistenceService;

import lombok.extern.slf4j.Slf4j;
import java.util.Map;

@Slf4j
public final class HistoryCompleteMessageStrategy implements CompleteMessageStrategy {
    private final PersistenceService persistenceService;
    private final PartitionManager partitionManager;
    private final String query;

    public HistoryCompleteMessageStrategy(
            SchemaName schemaName,
            SubscriptionId subscriptionId,
            PersistenceService persistenceService,
            PartitionManager partitionManager
    ) {
        this.persistenceService = persistenceService;
        this.partitionManager = partitionManager;
        this.query = new StringFormatter().execute("""
                                WITH deleted AS (
                                    DELETE FROM ${schema}.${subscriptionTable}
                                    WHERE id = ?
                                    RETURNING *
                                )
                        INSERT INTO ${schema}.${subscriptionHistoryTable}
                            (id, message_id, originated_at, attempt, status, error_message, stack_trace)
                            SELECT id, message_id, originated_at, attempt, 'PROCESSED' as status, error_message, stack_trace FROM deleted""",
                Map.of("schema", schemaName.value(),
                        "subscriptionTable", ConsumerTableNames.subscriptionTableName(subscriptionId),
                        "subscriptionHistoryTable", ConsumerTableNames.subscriptionHistoryTableName(subscriptionId)));
    }

    @Override
    public <T> void completeMessage(MessageContainer<T> messageContainer) throws DbqException {
        partitionManager.ensureHistoryPartitionExists(messageContainer.getOriginatedTime());

        var updated = persistenceService.update(query, messageContainer.getId());

        if (updated == 0) {
            log.warn("No records were updated by query '{}'", query);
        }
    }
}
