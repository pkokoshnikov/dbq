package org.pak.dbq.pg.consumer;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.internal.support.StringFormatter;
import org.pak.dbq.pg.PgQueryService;
import org.pak.dbq.pg.SchemaName;
import org.pak.dbq.api.SubscriptionId;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class DefaultRetryMessageStrategy implements RetryMessageStrategy {
    private final SchemaName schemaName;
    private final SubscriptionId subscriptionId;
    private final StringFormatter formatter = new StringFormatter();
    private final Map<String, String> queryCache = new ConcurrentHashMap<>();

    public DefaultRetryMessageStrategy(SchemaName schemaName, SubscriptionId subscriptionId) {
        this.schemaName = schemaName;
        this.subscriptionId = subscriptionId;
    }

    @Override
    public <T> void retryMessage(
            ConsumerQueryContext context,
            MessageContainer<T> messageContainer,
            Duration retryDuration,
            Exception e
    ) throws DbqException {
        var query = queryCache.computeIfAbsent("retryMessage|" + subscriptionId.id(), k -> formatter.execute("""
                        UPDATE ${schema}.${subscriptionTable} SET updated_at = CURRENT_TIMESTAMP,
                            execute_after = CURRENT_TIMESTAMP + (? * INTERVAL '1 second'), attempt = attempt + 1,
                            error_message = ?, stack_trace = ?
                        WHERE id = ?""",
                Map.of("schema", schemaName.value(),
                        "subscriptionTable", PgQueryService.subscriptionTableName(subscriptionId))));

        var updated = context.pgQueryService().persistenceService().update(query, retryDuration.getSeconds(),
                e.getMessage(), ExceptionUtils.getStackTrace(e), messageContainer.getId());

        context.pgQueryService().assertNonEmptyUpdate(updated, query);
    }
}
