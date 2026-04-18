package org.pak.dbq.spi;

import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.persistence.MessageContainer;

import java.time.Duration;
import java.util.List;

public interface ConsumerQueryService {
    <T> List<MessageContainer<T>> selectMessages() throws DbqException;

    <T> void retryMessage(MessageContainer<T> messageContainer, Duration retryDuration, Exception e) throws DbqException;

    <T> void failMessage(MessageContainer<T> messageContainer, Exception e) throws DbqException;

    <T> void completeMessage(MessageContainer<T> messageContainer) throws DbqException;
}
