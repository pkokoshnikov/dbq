package org.pak.dbq.internal.support;

import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.spi.TableManager;

public class NoOpTableManager implements TableManager {
    @Override
    public void registerQueue(QueueName queueName, int retentionDays) {
    }

    @Override
    public void registerSubscription(
            QueueName queueName,
            SubscriptionId subscriptionId,
            int retentionDays,
            boolean historyEnabled
    ) {
    }
}
