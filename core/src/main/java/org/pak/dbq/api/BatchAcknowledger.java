package org.pak.dbq.api;

import java.time.Duration;

public interface BatchAcknowledger<T> {
    void complete(MessageRecord<T> record);

    void retry(MessageRecord<T> record, Duration duration, Exception exception);

    void fail(MessageRecord<T> record, Exception exception);
}
