package org.pak.messagebus.core.service;

import org.pak.messagebus.core.Message;
import org.pak.messagebus.core.MessageContainer;
import org.pak.messagebus.core.QueueName;
import org.pak.messagebus.core.SubscriptionId;

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
