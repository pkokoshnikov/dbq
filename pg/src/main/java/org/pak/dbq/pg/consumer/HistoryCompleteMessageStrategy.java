package org.pak.dbq.pg.consumer;

import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.internal.support.StringFormatter;
import org.pak.dbq.pg.QueuePartitionService;
import org.pak.dbq.pg.SchemaName;
import org.pak.dbq.pg.TableNames;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.spi.PersistenceService;

import lombok.extern.slf4j.Slf4j;
import java.util.Map;

@Slf4j
public final class HistoryCompleteMessageStrategy implements CompleteMessageStrategy {
    private final SubscriptionId subscriptionId;
    private final PersistenceService persistenceService;
    private final QueuePartitionService queuePartitionService;
    private final String query;

    public HistoryCompleteMessageStrategy(
            SchemaName schemaName,
            SubscriptionId subscriptionId,
            PersistenceService persistenceService,
            QueuePartitionService queuePartitionService
    ) {
        this.subscriptionId = subscriptionId;
        this.persistenceService = persistenceService;
        this.queuePartitionService = queuePartitionService;
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
                        "subscriptionTable", TableNames.subscriptionTableName(subscriptionId),
                        "subscriptionHistoryTable", TableNames.subscriptionHistoryTableName(subscriptionId)));
    }

    @Override
    public <T> void completeMessage(MessageContainer<T> messageContainer) throws DbqException {
        queuePartitionService.ensureHistoryPartitionExists(messageContainer.getOriginatedTime(), subscriptionId);

        var updated = persistenceService.update(query, messageContainer.getId());

        if (updated == 0) {
            log.warn("No records were updated by query '{}'", query);
        }
    }
}
