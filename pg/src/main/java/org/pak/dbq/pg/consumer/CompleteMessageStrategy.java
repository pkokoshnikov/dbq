package org.pak.dbq.pg.consumer;

import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.persistence.MessageContainer;

public interface CompleteMessageStrategy {
    <T> void completeMessage(MessageContainer<T> messageContainer) throws DbqException;
}
