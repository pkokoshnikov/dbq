package org.pak.messagebus.core;

import org.pak.messagebus.core.error.PartitionHasReferencesException;
import org.pak.messagebus.core.service.QueryService;
import org.pak.messagebus.core.service.TransactionService;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Supplier;

class CoreTestSupport {
    static final MessageName MESSAGE_NAME = new MessageName("test-message");
    static final SubscriptionName SUBSCRIPTION_NAME = new SubscriptionName("test-subscription");

    record PartitionCreation<T>(T target, Instant includeDateTime) {
    }

    record SubscriptionRegistration(MessageName messageName, SubscriptionName subscriptionName, int storageDays) {
    }

    record CompletionCall(SubscriptionName subscriptionName, MessageContainer<?> messageContainer) {
    }

    record FailureCall(SubscriptionName subscriptionName, MessageContainer<?> messageContainer, Exception exception) {
    }

    record RetryCall(
            SubscriptionName subscriptionName,
            MessageContainer<?> messageContainer,
            Duration retryDuration,
            Exception exception
    ) {
    }

    record InsertCall<T>(MessageName messageName, Message<T> message) {
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
        final List<MessageName> initializedMessageTables = new ArrayList<>();
        final List<SubscriptionRegistration> subscriptionRegistrations = new ArrayList<>();
        final List<PartitionCreation<MessageName>> messagePartitionCreations = new ArrayList<>();
        final List<PartitionCreation<SubscriptionName>> historyPartitionCreations = new ArrayList<>();
        final List<CompletionCall> completions = new ArrayList<>();
        final List<FailureCall> failures = new ArrayList<>();
        final List<RetryCall> retries = new ArrayList<>();
        final List<InsertCall<?>> inserts = new ArrayList<>();
        final Queue<Object> insertMessageResults = new ArrayDeque<>();
        final Map<MessageName, List<LocalDate>> messagePartitions = new HashMap<>();
        final Map<SubscriptionName, List<LocalDate>> historyPartitions = new HashMap<>();
        final List<LocalDate> droppedMessagePartitions = new ArrayList<>();
        final List<LocalDate> droppedHistoryPartitions = new ArrayList<>();
        final List<LocalDate> messagePartitionsWithReferences = new ArrayList<>();
        final List<LocalDate> historyPartitionsWithReferences = new ArrayList<>();
        List<? extends MessageContainer<?>> selectedMessages = List.of();

        @Override
        public void initMessageTable(MessageName messageName) {
            initializedMessageTables.add(messageName);
        }

        @Override
        public void initSubscriptionTable(MessageName messageName, SubscriptionName subscriptionName) {
            subscriptionRegistrations.add(new SubscriptionRegistration(messageName, subscriptionName, -1));
        }

        @Override
        public void createMessagePartition(MessageName messageName, Instant includeDateTime) {
            messagePartitionCreations.add(new PartitionCreation<>(messageName, includeDateTime));
        }

        @Override
        public void createHistoryPartition(SubscriptionName messageName, Instant includeDateTime) {
            historyPartitionCreations.add(new PartitionCreation<>(messageName, includeDateTime));
        }

        @Override
        public List<LocalDate> getAllPartitions(MessageName messageName) {
            return messagePartitions.getOrDefault(messageName, List.of());
        }

        @Override
        public void dropMessagePartition(MessageName messageName, LocalDate partition) {
            if (messagePartitionsWithReferences.contains(partition)) {
                throw new PartitionHasReferencesException();
            }
            droppedMessagePartitions.add(partition);
        }

        @Override
        public void dropHistoryPartition(SubscriptionName messageName, LocalDate partition) {
            if (historyPartitionsWithReferences.contains(partition)) {
                throw new PartitionHasReferencesException();
            }
            droppedHistoryPartitions.add(partition);
        }

        @Override
        public List<LocalDate> getAllPartitions(SubscriptionName subscriptionName) {
            return historyPartitions.getOrDefault(subscriptionName, List.of());
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> List<MessageContainer<T>> selectMessages(
                MessageName messageName,
                SubscriptionName subscriptionName,
                Integer maxPollRecords
        ) {
            return (List<MessageContainer<T>>) selectedMessages;
        }

        @Override
        public <T> void retryMessage(
                SubscriptionName subscriptionName,
                MessageContainer<T> messageContainer,
                Duration retryDuration,
                Exception e
        ) {
            retries.add(new RetryCall(subscriptionName, messageContainer, retryDuration, e));
        }

        @Override
        public <T> void failMessage(SubscriptionName subscriptionName, MessageContainer<T> messageContainer, Exception e) {
            failures.add(new FailureCall(subscriptionName, messageContainer, e));
        }

        @Override
        public <T> void completeMessage(SubscriptionName subscriptionName, MessageContainer<T> messageContainer) {
            completions.add(new CompletionCall(subscriptionName, messageContainer));
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> boolean insertMessage(MessageName messageName, Message<T> message) {
            inserts.add(new InsertCall<>(messageName, message));
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
        public <T> List<Boolean> insertBatchMessage(MessageName messageName, List<Message<T>> messages) {
            return java.util.Collections.nCopies(messages.size(), Boolean.TRUE);
        }
    }
}
