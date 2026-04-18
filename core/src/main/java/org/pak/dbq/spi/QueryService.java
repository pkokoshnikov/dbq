package org.pak.dbq.spi;

import org.pak.dbq.api.Message;
import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.error.DbqException;

import java.time.Duration;
import java.util.List;

public interface QueryService {
    <T> List<MessageContainer<T>> selectMessages(
            QueueName queueName,
            SubscriptionId subscriptionId,
            Integer maxPollRecords,
            boolean serializedByKey
    ) throws DbqException;

    <T> void retryMessage(
            SubscriptionId subscriptionId,
            MessageContainer<T> messageContainer,
            Duration retryDuration,
            Exception e
    ) throws DbqException;

    <T> void failMessage(
            SubscriptionId subscriptionId,
            MessageContainer<T> messageContainer,
            Exception e,
            boolean historyEnabled
    ) throws DbqException;

    <T> void completeMessage(
            SubscriptionId subscriptionId,
            MessageContainer<T> messageContainer,
            boolean historyEnabled
    ) throws DbqException;

    <T> boolean insertMessage(QueueName queueName, Message<T> message) throws DbqException;

    <T> List<Boolean> insertBatchMessage(QueueName queueName, List<Message<T>> messages) throws DbqException;
}
