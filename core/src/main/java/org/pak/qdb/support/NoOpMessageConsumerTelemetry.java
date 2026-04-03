package org.pak.qdb.support;

import org.pak.qdb.api.QueueName;
import org.pak.qdb.api.SubscriptionId;
import org.pak.qdb.api.Message;
import org.pak.qdb.spi.MessageConsumerTelemetry;

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
