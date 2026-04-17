package org.pak.dbq.spi;

import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.spi.error.PersistenceException;

public interface TableManager {
    void registerQueue(QueueName queueName, int retentionDays, boolean autoDdl) throws PersistenceException;

    void registerSubscription(
            QueueName queueName,
            SubscriptionId subscriptionId,
            boolean historyEnabled,
            boolean serializedByKey
    ) throws PersistenceException;
}
