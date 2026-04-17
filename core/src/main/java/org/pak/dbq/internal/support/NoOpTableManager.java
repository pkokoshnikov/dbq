package org.pak.dbq.internal.support;

import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.spi.TableManager;
import org.pak.dbq.spi.error.PersistenceException;

public class NoOpTableManager implements TableManager {
    @Override
    public void registerQueue(QueueName queueName, int retentionDays, boolean autoDdl) throws PersistenceException {
    }

    @Override
    public void registerSubscription(
            QueueName queueName,
            SubscriptionId subscriptionId,
            boolean historyEnabled,
            boolean serializedByKey
    ) throws PersistenceException {
    }
}
