package org.pak.dbq.api;

import org.pak.dbq.error.DbqException;

import java.util.List;

public interface BatchMessageHandler<T> {
    void handle(List<MessageRecord<T>> messages, Acknowledger<T> acknowledger)
            throws DbqException, InterruptedException;
}
