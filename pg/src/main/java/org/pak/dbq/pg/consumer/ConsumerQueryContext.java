package org.pak.dbq.pg.consumer;

import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.pg.PgQueryService;

public record ConsumerQueryContext(
        PgQueryService pgQueryService,
        QueueName queueName,
        SubscriptionId subscriptionId,
        Integer maxPollRecords,
        boolean historyEnabled
) {
}
