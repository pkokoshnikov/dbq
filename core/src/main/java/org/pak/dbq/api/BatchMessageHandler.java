package org.pak.dbq.api;

import org.pak.dbq.spi.error.PersistenceException;

import java.util.List;

public interface BatchMessageHandler<T> {
    void handle(List<MessageRecord<T>> messages, Acknowledger<T> acknowledger)
            throws PersistenceException, InterruptedException;
}
