package org.pak.dbq.internal.support;

import org.pak.dbq.api.Message;
import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.spi.MessageConsumerTelemetry;

public final class NoOpMessageConsumerTelemetry implements MessageConsumerTelemetry {
    private static final Scope NO_OP_SCOPE = new Scope() {
        @Override
        public void recordError(Exception exception) {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }
    };

    @Override
    public <T> Scope start(Message<T> message, QueueName queueName, SubscriptionId subscriptionId) {
        return NO_OP_SCOPE;
    }
}
