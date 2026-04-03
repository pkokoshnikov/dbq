package org.pak.messagebus.core;

public interface MessageHandler<T> {
    void handle(Message<T> message);
}
