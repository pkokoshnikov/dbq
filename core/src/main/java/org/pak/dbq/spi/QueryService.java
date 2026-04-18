package org.pak.dbq.spi;

import org.pak.dbq.api.Message;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.error.DbqException;

import java.time.Duration;
import java.util.List;

public interface QueryService {
    <T> List<MessageContainer<T>> selectMessages() throws DbqException;

    <T> void retryMessage(MessageContainer<T> messageContainer, Duration retryDuration, Exception e) throws DbqException;

    <T> void failMessage(MessageContainer<T> messageContainer, Exception e) throws DbqException;

    <T> void completeMessage(MessageContainer<T> messageContainer) throws DbqException;

    <T> boolean insertMessage(Message<T> message) throws DbqException;

    <T> List<Boolean> insertBatchMessage(List<Message<T>> messages) throws DbqException;
}
