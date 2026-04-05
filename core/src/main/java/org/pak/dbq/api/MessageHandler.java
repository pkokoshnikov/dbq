package org.pak.dbq.api;

public interface MessageHandler<T> {
    void handle(Message<T> message);
}
