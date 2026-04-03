package org.pak.qdb.api;

public interface MessageHandler<T> {
    void handle(Message<T> message);
}
