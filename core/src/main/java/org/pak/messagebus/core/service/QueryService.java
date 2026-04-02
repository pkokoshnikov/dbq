package org.pak.messagebus.core.service;

import org.pak.messagebus.core.Message;
import org.pak.messagebus.core.MessageContainer;
import org.pak.messagebus.core.QueueName;
import org.pak.messagebus.core.SubscriptionName;

import java.time.Duration;
import java.util.List;

public interface QueryService {
    <T> List<MessageContainer<T>> selectMessages(QueueName queueName, SubscriptionName subscriptionName, Integer maxPollRecords);
    <T> void retryMessage(SubscriptionName subscriptionName, MessageContainer<T> messageContainer, Duration retryDuration, Exception e);
    <T> void failMessage(SubscriptionName subscriptionName, MessageContainer<T> messageContainer, Exception e);
    <T> void completeMessage(SubscriptionName subscriptionName, MessageContainer<T> messageContainer);
    <T> boolean insertMessage(QueueName queueName, Message<T> message);
    <T> List<Boolean> insertBatchMessage(QueueName queueName, List<Message<T>> messages);
}
