package org.pak.dbq.pg;

import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;

public final class TableNames {
    private TableNames() {
    }

    public static String queueTableName(QueueName queueName) {
        return queueName.name().replace("-", "_");
    }

    public static String subscriptionTableName(SubscriptionId subscriptionId) {
        return subscriptionId.id().replace("-", "_");
    }

    public static String subscriptionHistoryTableName(SubscriptionId subscriptionId) {
        return subscriptionTableName(subscriptionId) + "_history";
    }

    public static String subscriptionKeyLockTableName(SubscriptionId subscriptionId) {
        return subscriptionTableName(subscriptionId) + "_key_lock";
    }
}
