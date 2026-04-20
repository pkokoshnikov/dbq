package org.pak.dbq.pg.consumer;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.internal.support.StringFormatter;
import org.pak.dbq.pg.SchemaName;
import org.pak.dbq.pg.TableNames;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.spi.PersistenceService;

import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import java.util.Map;

@Slf4j
public final class DefaultRetryMessageStrategy implements RetryMessageStrategy {
    private final PersistenceService persistenceService;
    private final String query;

    public DefaultRetryMessageStrategy(
            SchemaName schemaName,
            SubscriptionId subscriptionId,
            PersistenceService persistenceService
    ) {
        this.persistenceService = persistenceService;
        this.query = new StringFormatter().execute("""
                        UPDATE ${schema}.${subscriptionTable} SET updated_at = CURRENT_TIMESTAMP,
                            execute_after = CURRENT_TIMESTAMP + (? * INTERVAL '1 second'), attempt = attempt + 1,
                            error_message = ?, stack_trace = ?
                        WHERE id = ?""",
                Map.of("schema", schemaName.value(),
                        "subscriptionTable", TableNames.subscriptionTableName(subscriptionId)));
    }

    @Override
    public <T> void retryMessage(
            MessageContainer<T> messageContainer,
            Duration retryDuration,
            Exception e
    ) throws DbqException {
        var updated = persistenceService.update(query, retryDuration.getSeconds(),
                e.getMessage(), ExceptionUtils.getStackTrace(e), messageContainer.getId());

        if (updated == 0) {
            log.warn("No records were updated by query '{}'", query);
        }
    }
}
