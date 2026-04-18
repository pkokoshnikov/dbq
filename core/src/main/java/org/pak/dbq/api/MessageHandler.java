package org.pak.dbq.api;

import org.pak.dbq.error.DbqException;

public interface MessageHandler<T> {
    void handle(Message<T> message) throws DbqException, InterruptedException;
}
