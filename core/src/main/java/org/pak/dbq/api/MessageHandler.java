package org.pak.dbq.api;

import org.pak.dbq.spi.error.PersistenceException;

public interface MessageHandler<T> {
    void handle(Message<T> message) throws PersistenceException, InterruptedException;
}
