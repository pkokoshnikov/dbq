package org.pak.dbq.internal;

import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.api.Message;
import org.pak.dbq.api.ProducerConfig;
import org.pak.dbq.spi.ConsumerQueryService;
import org.pak.dbq.error.DbqException;
import org.pak.dbq.spi.MessageConsumerTelemetry;
import org.pak.dbq.spi.MessageContextPropagator;
import org.pak.dbq.spi.ProducerQueryService;
import org.pak.dbq.spi.QueryServiceFactory;
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
        public void registerQueue(QueueName queueName, int retentionDays, boolean autoDdl) throws DbqException {
            queueRegistrations.add(new QueueRegistrationCall(queueName, retentionDays, autoDdl));
        }

        @Override
        public void registerSubscription(
                QueueName queueName,
                SubscriptionId subscriptionId,
                boolean historyEnabled,
                boolean serializedByKey
        ) throws DbqException {
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

    private static final class RecordingQueryState {
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
    }

    public static class RecordingQueryService implements ProducerQueryService, ConsumerQueryService {
        private final RecordingQueryState state;
        private final QueueName queueName;
        private final SubscriptionId subscriptionId;
        private final boolean historyEnabled;
        private final boolean serializedByKey;

        public RecordingQueryService() {
            this(new RecordingQueryState(), null, null, false, false);
        }

        public RecordingQueryService(QueueName queueName) {
            this(new RecordingQueryState(), queueName, null, false, false);
        }

        public RecordingQueryService(
                QueueName queueName,
                SubscriptionId subscriptionId,
                boolean historyEnabled,
                boolean serializedByKey
        ) {
            this(new RecordingQueryState(), queueName, subscriptionId, historyEnabled, serializedByKey);
        }

        private RecordingQueryService(
                RecordingQueryState state,
                QueueName queueName,
                SubscriptionId subscriptionId,
                boolean historyEnabled,
                boolean serializedByKey
        ) {
            this.state = state;
            this.queueName = queueName;
            this.subscriptionId = subscriptionId;
            this.historyEnabled = historyEnabled;
            this.serializedByKey = serializedByKey;
        }

        public void setSelectedMessages(List<? extends MessageContainer<?>> selectedMessages) {
            state.selectedMessages = List.copyOf(selectedMessages);
        }

        public List<CompletionCall> getCompletions() {
            return state.completions;
        }

        public List<FailureCall> getFailures() {
            return state.failures;
        }

        public List<RetryCall> getRetries() {
            return state.retries;
        }

        public List<InsertCall<?>> getInserts() {
            return state.inserts;
        }

        public List<BatchInsertCall<?>> getBatchInserts() {
            return state.batchInserts;
        }

        public boolean isLastSerializedByKey() {
            return state.lastSerializedByKey;
        }

        public void enqueueInsertMessageResult(Object result) {
            state.insertMessageResults.add(result);
        }

        public void enqueueCompleteMessageResult(Object result) {
            state.completeMessageResults.add(result);
        }

        public void enqueueRetryMessageResult(Object result) {
            state.retryMessageResults.add(result);
        }

        public void enqueueFailMessageResult(Object result) {
            state.failMessageResults.add(result);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> List<MessageContainer<T>> selectMessages() throws DbqException {
            state.lastSerializedByKey = serializedByKey;
            return (List<MessageContainer<T>>) state.selectedMessages;
        }

        @Override
        public <T> void retryMessage(MessageContainer<T> messageContainer, Duration retryDuration, Exception e)
                throws DbqException {
            Object result = state.retryMessageResults.poll();
            if (result instanceof Throwable throwable) {
                sneakyThrow(throwable);
            }
            state.retries.add(new RetryCall(subscriptionId, messageContainer, retryDuration, e));
        }

        @Override
        public <T> void failMessage(MessageContainer<T> messageContainer, Exception e) throws DbqException {
            Object result = state.failMessageResults.poll();
            if (result instanceof Throwable throwable) {
                sneakyThrow(throwable);
            }
            state.failures.add(new FailureCall(subscriptionId, messageContainer, e, historyEnabled));
        }

        @Override
        public <T> void completeMessage(MessageContainer<T> messageContainer) throws DbqException {
            Object result = state.completeMessageResults.poll();
            if (result instanceof Throwable throwable) {
                sneakyThrow(throwable);
            }
            state.completions.add(new CompletionCall(subscriptionId, messageContainer, historyEnabled));
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> boolean insertMessage(Message<T> message) throws DbqException {
            state.inserts.add(new InsertCall<>(queueName, message));
            Object result = state.insertMessageResults.poll();
            if (result instanceof Throwable throwable) {
                sneakyThrow(throwable);
            }
            if (result instanceof Boolean bool) {
                return bool;
            }
            return true;
        }

        @Override
        public <T> List<Boolean> insertBatchMessage(List<Message<T>> messages) throws DbqException {
            state.batchInserts.add(new BatchInsertCall<>(queueName, List.copyOf(messages)));
            return java.util.Collections.nCopies(messages.size(), Boolean.TRUE);
        }
    }

    public static class RecordingQueryServiceFactory implements QueryServiceFactory {
        private final RecordingQueryState state = new RecordingQueryState();

        @Override
        public ProducerQueryService createProducerQueryService(ProducerConfig<?> producerConfig) {
            return new RecordingQueryService(state, producerConfig.getQueueName(), null, false, false);
        }

        @Override
        public ConsumerQueryService createConsumerQueryService(ConsumerConfig<?> consumerConfig) {
            return new RecordingQueryService(
                    state,
                    consumerConfig.getQueueName(),
                    consumerConfig.getSubscriptionId(),
                    consumerConfig.getProperties().isHistoryEnabled(),
                    consumerConfig.getProperties().isSerializedByKey());
        }

        public List<CompletionCall> getCompletions() {
            return state.completions;
        }

        public List<FailureCall> getFailures() {
            return state.failures;
        }

        public List<RetryCall> getRetries() {
            return state.retries;
        }

        public List<InsertCall<?>> getInserts() {
            return state.inserts;
        }

        public List<BatchInsertCall<?>> getBatchInserts() {
            return state.batchInserts;
        }

        public boolean isLastSerializedByKey() {
            return state.lastSerializedByKey;
        }

        public void setSelectedMessages(List<? extends MessageContainer<?>> selectedMessages) {
            state.selectedMessages = List.copyOf(selectedMessages);
        }

        public void enqueueInsertMessageResult(Object result) {
            state.insertMessageResults.add(result);
        }

        public void enqueueCompleteMessageResult(Object result) {
            state.completeMessageResults.add(result);
        }

        public void enqueueRetryMessageResult(Object result) {
            state.retryMessageResults.add(result);
        }

        public void enqueueFailMessageResult(Object result) {
            state.failMessageResults.add(result);
        }
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void sneakyThrow(Throwable throwable) throws E {
        throw (E) throwable;
    }
}
