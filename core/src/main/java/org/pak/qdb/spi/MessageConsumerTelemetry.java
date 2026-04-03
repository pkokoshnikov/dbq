package org.pak.qdb.spi;

import org.pak.qdb.api.QueueName;
import org.pak.qdb.api.SubscriptionId;
import org.pak.qdb.api.Message;

public interface MessageConsumerTelemetry {
    <T> Scope start(Message<T> message, QueueName queueName, SubscriptionId subscriptionId);

    interface Scope extends AutoCloseable {
        void recordError(Exception exception);

        @Override
        void close();
    }
}
