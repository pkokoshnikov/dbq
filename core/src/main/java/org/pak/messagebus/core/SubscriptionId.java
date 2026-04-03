package org.pak.messagebus.core;

import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
public class SubscriptionId {
    private final String subscriptionId;

    public SubscriptionId(String subscriptionId) {
        if (!subscriptionId.matches("^[a-z-]+$")) {
            throw new IllegalArgumentException("Subscription id must be lowercase and -");
        }

        this.subscriptionId = subscriptionId;
    }

    public String id() {
        return subscriptionId;
    }

    @Override
    public String toString() {
        return subscriptionId;
    }
}
