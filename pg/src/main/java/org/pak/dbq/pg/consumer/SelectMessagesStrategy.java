package org.pak.dbq.pg.consumer;

import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.persistence.MessageContainer;

import java.util.List;

public interface SelectMessagesStrategy {
    <T> List<MessageContainer<T>> selectMessages() throws DbqException;
}
