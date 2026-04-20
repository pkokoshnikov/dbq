package org.pak.dbq.pg.consumer;

import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.persistence.MessageContainer;

public interface FailMessageStrategy {
    <T> void failMessage(ConsumerQueryContext context, MessageContainer<T> messageContainer, Exception e)
            throws DbqException;
}
