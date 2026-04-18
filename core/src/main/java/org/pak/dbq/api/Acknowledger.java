package org.pak.dbq.api;

import org.pak.dbq.error.DbqException;

import java.time.Duration;

public interface Acknowledger<T> {
    void complete(MessageRecord<T> record) throws DbqException;

    void retry(MessageRecord<T> record, Duration duration, Exception exception) throws DbqException;

    void fail(MessageRecord<T> record, Exception exception) throws DbqException;
}
