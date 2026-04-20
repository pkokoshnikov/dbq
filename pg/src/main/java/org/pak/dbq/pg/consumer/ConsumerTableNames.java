package org.pak.dbq.pg.consumer;

import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;

public final class ConsumerTableNames {
    private ConsumerTableNames() {
    }

    public static String queueTableName(QueueName queueName) {
        return queueName.name().replace("-", "_");
    }

    public static String subscriptionTableName(SubscriptionId subscriptionId) {
        return subscriptionId.id().replace("-", "_");
    }

    public static String subscriptionHistoryTableName(SubscriptionId subscriptionId) {
        return subscriptionId.id().replace("-", "_") + "_history";
    }

    public static String subscriptionKeyLockTableName(SubscriptionId subscriptionId) {
        return subscriptionId.id().replace("-", "_") + "_key_lock";
    }
}
