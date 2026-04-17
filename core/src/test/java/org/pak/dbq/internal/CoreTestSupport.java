package org.pak.dbq.internal;

import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.api.Message;
import org.pak.dbq.spi.MessageConsumerTelemetry;
import org.pak.dbq.spi.MessageContextPropagator;
import org.pak.dbq.spi.QueryService;
import org.pak.dbq.spi.TableManager;
import org.pak.dbq.spi.TransactionService;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Supplier;

public class CoreTestSupport {
    public static final QueueName QUEUE_NAME = new QueueName("test-message");
    public static final SubscriptionId SUBSCRIPTION_NAME = new SubscriptionId("test-subscription");

    public record CompletionCall(
            SubscriptionId subscriptionId,
            MessageContainer<?> messageContainer,
            boolean historyEnabled
    ) {
    }

    public record FailureCall(
            SubscriptionId subscriptionId,
            MessageContainer<?> messageContainer,
            Exception exception,
            boolean historyEnabled
    ) {
    }

    public record RetryCall(
            SubscriptionId subscriptionId,
            MessageContainer<?> messageContainer,
            Duration retryDuration,
            Exception exception
    ) {
    }

    public record InsertCall<T>(QueueName queueName, Message<T> message) {
    }

    public record BatchInsertCall<T>(QueueName queueName, List<Message<T>> messages) {
    }

    public record QueueRegistrationCall(QueueName queueName, int retentionDays, boolean autoDdl) {
    }

    public record SubscriptionRegistrationCall(
            QueueName queueName,
            SubscriptionId subscriptionId,
            boolean historyEnabled,
            boolean serializedByKey
    ) {
    }

    public static MessageContainer<String> messageContainer(String payload, int attempt, Instant originatedTime) {
        return messageContainer(java.math.BigInteger.ONE, java.math.BigInteger.TWO, "key-1",
                payload, Map.of(), attempt, originatedTime);
    }

    public static MessageContainer<String> messageContainer(
            String payload,
            Map<String, String> headers,
            int attempt,
            Instant originatedTime
    ) {
        return messageContainer(java.math.BigInteger.ONE, java.math.BigInteger.TWO, "key-1",
                payload, headers, attempt, originatedTime);
    }

    public static MessageContainer<String> messageContainer(
            java.math.BigInteger id,
            java.math.BigInteger messageId,
            String key,
            String payload,
            Map<String, String> headers,
            int attempt,
            Instant originatedTime
    ) {
        return new MessageContainer<>(
                id,
                messageId,
                key,
                attempt,
                originatedTime,
                originatedTime,
                null,
                originatedTime,
                payload,
                headers,
                null,
                null
        );
    }

    public static final class RecordingMessageContextPropagator implements MessageContextPropagator {
        private final Map<String, String> injectedHeaders;
        private Map<String, String> extractedHeaders = Map.of();
        private boolean scopeClosed;

        public RecordingMessageContextPropagator(Map<String, String> injectedHeaders) {
            this.injectedHeaders = Map.copyOf(injectedHeaders);
        }

        @Override
        public Map<String, String> injectCurrentContext(Map<String, String> headers) {
            var result = new java.util.LinkedHashMap<>(headers);
            result.putAll(injectedHeaders);
            return Map.copyOf(result);
        }

        @Override
        public Scope extractToCurrentContext(Map<String, String> headers) {
            extractedHeaders = Map.copyOf(headers);
            scopeClosed = false;
            return () -> scopeClosed = true;
        }

        public Map<String, String> getExtractedHeaders() {
            return extractedHeaders;
        }

        public boolean isScopeClosed() {
            return scopeClosed;
        }
    }


    public static final class RecordingMessageConsumerTelemetry implements MessageConsumerTelemetry {
        private Message<?> startedMessage;
        private QueueName startedQueueName;
        private SubscriptionId startedSubscriptionId;
        private Exception recordedException;
        private boolean scopeClosed;

        @Override
        public <T> Scope start(Message<T> message, QueueName queueName, SubscriptionId subscriptionId) {
            startedMessage = message;
            startedQueueName = queueName;
            startedSubscriptionId = subscriptionId;
            scopeClosed = false;
            return new Scope() {
                @Override
                public void recordError(Exception exception) {
                    recordedException = exception;
                }

                @Override
                public void close() {
                    scopeClosed = true;
                }
            };
        }

        public Message<?> startedMessage() {
            return startedMessage;
        }

        public QueueName startedQueueName() {
            return startedQueueName;
        }

        public SubscriptionId startedSubscriptionId() {
            return startedSubscriptionId;
        }

        public Exception recordedException() {
            return recordedException;
        }

        public boolean isScopeClosed() {
            return scopeClosed;
        }
    }

    public static class DirectTransactionService implements TransactionService {
        int calls;

        @Override
        public <T> T inTransaction(Supplier<T> runnable) {
            calls++;
            return runnable.get();
        }
    }

    public static class RecordingTableManager implements TableManager {
        private final List<QueueRegistrationCall> queueRegistrations = new ArrayList<>();
        private final List<SubscriptionRegistrationCall> subscriptionRegistrations = new ArrayList<>();

        @Override
        public void registerQueue(QueueName queueName, int retentionDays, boolean autoDdl) {
            queueRegistrations.add(new QueueRegistrationCall(queueName, retentionDays, autoDdl));
        }

        @Override
        public void registerSubscription(
                QueueName queueName,
                SubscriptionId subscriptionId,
                boolean historyEnabled,
                boolean serializedByKey
        ) {
            subscriptionRegistrations.add(new SubscriptionRegistrationCall(
                    queueName,
                    subscriptionId,
                    historyEnabled,
                    serializedByKey));
        }

        public List<QueueRegistrationCall> getQueueRegistrations() {
            return queueRegistrations;
        }

        public List<SubscriptionRegistrationCall> getSubscriptionRegistrations() {
            return subscriptionRegistrations;
        }
    }

    public static class RecordingQueryService implements QueryService {
        private final List<CompletionCall> completions = new ArrayList<>();
        private final List<FailureCall> failures = new ArrayList<>();
        private final List<RetryCall> retries = new ArrayList<>();
        private final List<InsertCall<?>> inserts = new ArrayList<>();
        private final List<BatchInsertCall<?>> batchInserts = new ArrayList<>();
        private final Queue<Object> insertMessageResults = new ArrayDeque<>();
        private final Queue<Object> completeMessageResults = new ArrayDeque<>();
        private final Queue<Object> retryMessageResults = new ArrayDeque<>();
        private final Queue<Object> failMessageResults = new ArrayDeque<>();
        private List<? extends MessageContainer<?>> selectedMessages = List.of();
        private boolean lastSerializedByKey;

        public void setSelectedMessages(List<? extends MessageContainer<?>> selectedMessages) {
            this.selectedMessages = List.copyOf(selectedMessages);
        }

        public List<CompletionCall> getCompletions() {
            return completions;
        }

        public List<FailureCall> getFailures() {
            return failures;
        }

        public List<RetryCall> getRetries() {
            return retries;
        }

        public List<InsertCall<?>> getInserts() {
            return inserts;
        }

        public List<BatchInsertCall<?>> getBatchInserts() {
            return batchInserts;
        }

        public boolean isLastSerializedByKey() {
            return lastSerializedByKey;
        }

        public void enqueueInsertMessageResult(Object result) {
            insertMessageResults.add(result);
        }

        public void enqueueCompleteMessageResult(Object result) {
            completeMessageResults.add(result);
        }

        public void enqueueRetryMessageResult(Object result) {
            retryMessageResults.add(result);
        }

        public void enqueueFailMessageResult(Object result) {
            failMessageResults.add(result);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> List<MessageContainer<T>> selectMessages(
                QueueName queueName,
                SubscriptionId subscriptionId,
                Integer maxPollRecords,
                boolean serializedByKey
        ) {
            lastSerializedByKey = serializedByKey;
            return (List<MessageContainer<T>>) selectedMessages;
        }

        @Override
        public <T> void retryMessage(
                SubscriptionId subscriptionId,
                MessageContainer<T> messageContainer,
                Duration retryDuration,
                Exception e
        ) {
            Object result = retryMessageResults.poll();
            if (result instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            retries.add(new RetryCall(subscriptionId, messageContainer, retryDuration, e));
        }

        @Override
        public <T> void failMessage(
                SubscriptionId subscriptionId,
                MessageContainer<T> messageContainer,
                Exception e,
                boolean historyEnabled
        ) {
            Object result = failMessageResults.poll();
            if (result instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            failures.add(new FailureCall(subscriptionId, messageContainer, e, historyEnabled));
        }

        @Override
        public <T> void completeMessage(
                SubscriptionId subscriptionId,
                MessageContainer<T> messageContainer,
                boolean historyEnabled
        ) {
            Object result = completeMessageResults.poll();
            if (result instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            completions.add(new CompletionCall(subscriptionId, messageContainer, historyEnabled));
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
