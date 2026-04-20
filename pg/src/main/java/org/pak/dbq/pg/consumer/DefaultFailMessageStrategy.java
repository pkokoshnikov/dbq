package org.pak.dbq.pg.consumer;

import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.internal.support.StringFormatter;
import org.pak.dbq.pg.SchemaName;
import org.pak.dbq.pg.TableNames;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.spi.PersistenceService;

import lombok.extern.slf4j.Slf4j;
import java.util.Map;

@Slf4j
public final class DefaultFailMessageStrategy implements FailMessageStrategy {
    private final PersistenceService persistenceService;
    private final String query;

    public DefaultFailMessageStrategy(
            SchemaName schemaName,
            SubscriptionId subscriptionId,
            PersistenceService persistenceService
    ) {
        this.persistenceService = persistenceService;
        this.query = new StringFormatter().execute("""
                        DELETE FROM ${schema}.${subscriptionTable} WHERE id = ?""",
                Map.of("schema", schemaName.value(),
                        "subscriptionTable", TableNames.subscriptionTableName(subscriptionId)));
    }

    @Override
    public <T> void failMessage(MessageContainer<T> messageContainer, Exception e) throws DbqException {
        var updated = persistenceService.update(query, messageContainer.getId());
        if (updated == 0) {
            log.warn("No records were updated by query '{}'", query);
        }
    }
}
