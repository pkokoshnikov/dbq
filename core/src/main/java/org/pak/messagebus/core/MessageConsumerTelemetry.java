package org.pak.messagebus.core;

public interface MessageConsumerTelemetry {
    <T> Scope start(Message<T> message, QueueName queueName, SubscriptionId subscriptionId);

    interface Scope extends AutoCloseable {
        void recordError(Exception exception);

        @Override
        void close();
    }
}
