package org.pak.dbq.spi;

import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.api.Message;

import java.time.Duration;
import java.util.List;

public interface QueryService {
    <T> List<MessageContainer<T>> selectMessages(QueueName queueName, SubscriptionId subscriptionId, Integer maxPollRecords);
    <T> void retryMessage(SubscriptionId subscriptionId, MessageContainer<T> messageContainer, Duration retryDuration, Exception e);
    <T> void failMessage(SubscriptionId subscriptionId, MessageContainer<T> messageContainer, Exception e);
    <T> void completeMessage(SubscriptionId subscriptionId, MessageContainer<T> messageContainer);
    <T> boolean insertMessage(QueueName queueName, Message<T> message);
    <T> List<Boolean> insertBatchMessage(QueueName queueName, List<Message<T>> messages);
}
