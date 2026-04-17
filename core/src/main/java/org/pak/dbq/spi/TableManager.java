package org.pak.dbq.spi;

import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;

public interface TableManager {
    void registerQueue(QueueName queueName, int retentionDays, boolean autoDdl);

    void registerSubscription(
            QueueName queueName,
            SubscriptionId subscriptionId,
            boolean historyEnabled,
            boolean serializedByKey
    );
}
