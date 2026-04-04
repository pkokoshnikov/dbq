package org.pak.qdb.spi;

import org.pak.qdb.api.QueueName;
import org.pak.qdb.api.SubscriptionId;
import org.pak.qdb.internal.persistence.MessageContainer;
import org.pak.qdb.api.Message;

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
