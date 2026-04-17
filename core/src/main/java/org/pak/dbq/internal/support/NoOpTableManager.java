package org.pak.dbq.internal.support;

import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.spi.TableManager;

public class NoOpTableManager implements TableManager {
    @Override
    public void registerQueue(QueueName queueName, int retentionDays, boolean autoDdl) {
    }

    @Override
    public void registerSubscription(
            QueueName queueName,
            SubscriptionId subscriptionId,
            boolean historyEnabled,
            boolean serializedByKey
    ) {
    }
}
