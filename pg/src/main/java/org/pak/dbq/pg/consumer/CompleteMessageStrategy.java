package org.pak.dbq.pg.consumer;

import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.persistence.MessageContainer;

interface CompleteMessageStrategy {
    <T> void completeMessage(ConsumerQueryContext context, MessageContainer<T> messageContainer) throws DbqException;
}
