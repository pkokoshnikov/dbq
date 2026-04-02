package org.pak.messagebus.core.service;

import org.pak.messagebus.core.Message;
import org.pak.messagebus.core.MessageContainer;
import org.pak.messagebus.core.MessageName;
import org.pak.messagebus.core.SubscriptionName;

import java.time.Duration;
import java.util.List;

public interface QueryService {
    <T> List<MessageContainer<T>> selectMessages(MessageName messageName, SubscriptionName subscriptionName, Integer maxPollRecords);
    <T> void retryMessage(SubscriptionName subscriptionName, MessageContainer<T> messageContainer, Duration retryDuration, Exception e);
    <T> void failMessage(SubscriptionName subscriptionName, MessageContainer<T> messageContainer, Exception e);
    <T> void completeMessage(SubscriptionName subscriptionName, MessageContainer<T> messageContainer);
    <T> boolean insertMessage(MessageName messageName, Message<T> message);
    <T> List<Boolean> insertBatchMessage(MessageName messageName, List<Message<T>> messages);
}
