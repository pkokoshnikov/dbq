package org.pak.messagebus.core;

import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
public class QueueName {
    private final String queueName;

    public QueueName(String queueName) {
        if (!queueName.matches("^[a-z-]+$")) {
            throw new IllegalArgumentException("Queue name must be lowercase and -");
        }

        this.queueName = queueName;
    }

    public String name() {
        return queueName;
    }

    @Override
    public String toString() {
        return queueName;
    }
}
