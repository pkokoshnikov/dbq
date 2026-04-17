package org.pak.dbq.api;

import org.pak.dbq.spi.error.PersistenceException;

import java.time.Duration;

public interface BatchAcknowledger<T> {
    void complete(MessageRecord<T> record) throws PersistenceException;

    void retry(MessageRecord<T> record, Duration duration, Exception exception) throws PersistenceException;

    void fail(MessageRecord<T> record, Exception exception) throws PersistenceException;
}
