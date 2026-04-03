package org.pak.messagebus.core;

import java.time.Instant;

public class StdMessageFactory implements MessageFactory {
    @Override
    public <T> Message<T> createMessage(String key, Instant originatedTime, T payload) {
        return new SimpleMessage<>(key, originatedTime, payload);
    }
}
