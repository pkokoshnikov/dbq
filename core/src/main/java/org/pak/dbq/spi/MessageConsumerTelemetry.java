package org.pak.dbq.spi;

import org.pak.dbq.api.Message;
import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;

public interface MessageConsumerTelemetry {
    <T> Scope start(Message<T> message, QueueName queueName, SubscriptionId subscriptionId);

    interface Scope extends AutoCloseable {
        void recordError(Exception exception);

        @Override
        void close();
    }
}
