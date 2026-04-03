package org.pak.messagebus.core;

import org.pak.messagebus.core.service.QueryService;
import org.pak.messagebus.core.service.TransactionService;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.function.Supplier;

class CoreTestSupport {
    static final QueueName QUEUE_NAME = new QueueName("test-message");
    static final SubscriptionId SUBSCRIPTION_NAME = new SubscriptionId("test-subscription");

    record CompletionCall(SubscriptionId subscriptionId, MessageContainer<?> messageContainer) {
    }

    record FailureCall(SubscriptionId subscriptionId, MessageContainer<?> messageContainer, Exception exception) {
    }

    record RetryCall(
            SubscriptionId subscriptionId,
            MessageContainer<?> messageContainer,
            Duration retryDuration,
            Exception exception
    ) {
    }

    record InsertCall<T>(QueueName queueName, Message<T> message) {
    }

    record BatchInsertCall<T>(QueueName queueName, List<Message<T>> messages) {
    }

    static MessageContainer<String> messageContainer(String payload, int attempt, Instant originatedTime) {
        return new MessageContainer<>(
                java.math.BigInteger.ONE,
                java.math.BigInteger.TWO,
                "key-1",
                attempt,
                originatedTime,
                originatedTime,
                null,
                originatedTime,
                payload,
                null,
                null
        );
    }

    static class DirectTransactionService implements TransactionService {
        int calls;

        @Override
        public <T> T inTransaction(Supplier<T> runnable) {
            calls++;
            return runnable.get();
        }
    }

    static class RecordingQueryService implements QueryService {
        final List<CompletionCall> completions = new ArrayList<>();
        final List<FailureCall> failures = new ArrayList<>();
        final List<RetryCall> retries = new ArrayList<>();
        final List<InsertCall<?>> inserts = new ArrayList<>();
        final List<BatchInsertCall<?>> batchInserts = new ArrayList<>();
        final Queue<Object> insertMessageResults = new ArrayDeque<>();
        List<? extends MessageContainer<?>> selectedMessages = List.of();

        @SuppressWarnings("unchecked")
        @Override
        public <T> List<MessageContainer<T>> selectMessages(
                QueueName queueName,
                SubscriptionId subscriptionId,
                Integer maxPollRecords
        ) {
            return (List<MessageContainer<T>>) selectedMessages;
        }

        @Override
        public <T> void retryMessage(
                SubscriptionId subscriptionId,
                MessageContainer<T> messageContainer,
                Duration retryDuration,
                Exception e
        ) {
            retries.add(new RetryCall(subscriptionId, messageContainer, retryDuration, e));
        }

        @Override
        public <T> void failMessage(SubscriptionId subscriptionId, MessageContainer<T> messageContainer, Exception e) {
            failures.add(new FailureCall(subscriptionId, messageContainer, e));
        }

        @Override
        public <T> void completeMessage(SubscriptionId subscriptionId, MessageContainer<T> messageContainer) {
            completions.add(new CompletionCall(subscriptionId, messageContainer));
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> boolean insertMessage(QueueName queueName, Message<T> message) {
            inserts.add(new InsertCall<>(queueName, message));
            Object result = insertMessageResults.poll();
            if (result instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (result instanceof Boolean bool) {
                return bool;
            }
            return true;
        }

        @Override
        public <T> List<Boolean> insertBatchMessage(QueueName queueName, List<Message<T>> messages) {
            batchInserts.add(new BatchInsertCall<>(queueName, List.copyOf(messages)));
            return java.util.Collections.nCopies(messages.size(), Boolean.TRUE);
        }
    }
}
