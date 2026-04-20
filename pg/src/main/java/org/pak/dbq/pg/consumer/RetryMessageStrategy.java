package org.pak.dbq.pg.consumer;

import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.persistence.MessageContainer;

import java.time.Duration;

public interface RetryMessageStrategy {
    <T> void retryMessage(
            ConsumerQueryContext context,
            MessageContainer<T> messageContainer,
            Duration retryDuration,
            Exception e
    ) throws DbqException;
}
