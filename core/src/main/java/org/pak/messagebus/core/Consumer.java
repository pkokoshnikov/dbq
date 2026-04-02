package org.pak.messagebus.core;

public interface Consumer<T> {
    void handle(Message<T> message);
}
