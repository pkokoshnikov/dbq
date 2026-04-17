package org.pak.dbq.internal.consumer;

import lombok.NonNull;
import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.Message;
import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.error.MessageSerializationException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.spi.MessageConsumerTelemetry;
import org.pak.dbq.spi.MessageContextPropagator;
import org.pak.dbq.spi.MessageFactory;
import org.pak.dbq.spi.QueryService;
import org.pak.dbq.spi.TransactionService;
import org.pak.dbq.spi.error.NonRetrayablePersistenceException;
import org.pak.dbq.spi.error.RetryablePersistenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractConsumer<T> {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final String id = UUID.randomUUID().toString();
    private final QueryService queryService;
    private final QueueName queueName;
    private final SubscriptionId subscriptionId;
    private final TransactionService transactionService;
    private final MessageContextPropagator messageContextPropagator;
    private final MessageConsumerTelemetry messageConsumerTelemetry;
    private Duration pause = null;
    private final Duration unpredictedExceptionPause;
    private final MessageFactory messageFactory;
    private final Integer maxPollRecords;
    private final Duration persistenceExceptionPause;
    private final boolean historyEnabled;
    private final boolean serializedByKey;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    protected AbstractConsumer(
            @NonNull QueueName queueName,
            @NonNull SubscriptionId subscriptionId,
            @NonNull QueryService queryService,
            @NonNull TransactionService transactionService,
            @NonNull MessageContextPropagator messageContextPropagator,
            @NonNull MessageConsumerTelemetry messageConsumerTelemetry,
            @NonNull MessageFactory messageFactory,
            @NonNull ConsumerConfig.Properties properties
    ) {
        this.queryService = queryService;
        this.queueName = queueName;
        this.subscriptionId = subscriptionId;
        this.transactionService = transactionService;
        this.messageContextPropagator = messageContextPropagator;
        this.messageConsumerTelemetry = messageConsumerTelemetry;
        this.messageFactory = messageFactory;
        this.maxPollRecords = properties.getMaxPollRecords();
        this.persistenceExceptionPause = properties.getPersistenceExceptionPause();
        this.unpredictedExceptionPause = properties.getUnpredictedExceptionPause();
        this.historyEnabled = properties.isHistoryEnabled();
        this.serializedByKey = properties.isSerializedByKey();
    }

    public void poolLoop() {
        if (!isRunning.compareAndSet(false, true)) {
            log.warn("Consumer should be started only once");
            return;
        }

        try (var ignoreExecutorIdMDC = MDC.putCloseable("messageProcessorId", id);
                var ignoredEventNameMDC = MDC.putCloseable("queueName", queueName.name());
                var ignoredSubscriptionMDC = MDC.putCloseable("subscriptionId", subscriptionId.id())) {
            do {
                log.info("Start pooling");
                try {
                    do {
                        if (pause != null) {
                            log.info("Pause pooling {}", pause);
                            Thread.sleep(pause.toMillis());
                            pause = null;
                        }

                        var isPooled = transactionService.inTransaction(this::poolAndProcess);

                        if (Boolean.FALSE.equals(isPooled)) {
                            Thread.sleep(50);
                        }
                    } while (isRunning.get());
                } catch (MessageSerializationException e) {
                    log.error("Serializer exception occurred, we cannot skip messages", e);
                    isRunning.set(false);
                } catch (NonRetrayablePersistenceException e) {
                    log.error("Non recoverable persistence exception occurred, stop processing", e);
                    isRunning.set(false);
                } catch (RetryablePersistenceException e) {
                    log.warn("Retryable persistence exception occurred, pause processing", e);
                    pause = persistenceExceptionPause;
                } catch (InterruptedException e) {
                    log.warn("Consumer is interrupted", e);

                    Thread.currentThread().interrupt();
                    isRunning.set(false);
                } catch (Exception e) {
                    log.error("Unexpected exception occurred", e);
                    pause = unpredictedExceptionPause;
                }
            } while (isRunning.get());

            log.info("Consumer is stopped");
        }
    }

    public boolean poolAndProcess() {
        List<MessageContainer<T>> messageContainerList =
                queryService.selectMessages(queueName, subscriptionId, maxPollRecords, serializedByKey);

        if (messageContainerList.isEmpty()) {
            return false;
        }

        processMessages(messageContainerList);
        return true;
    }

    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            log.info("Prepare to stop consumer");
        } else {
            log.warn("Consumer should be stopped only once");
        }
    }

    protected abstract void processMessages(List<MessageContainer<T>> messageContainerList);

    protected Message<T> toMessage(MessageContainer<T> messageContainer) {
        return messageFactory.createMessage(
                messageContainer.getKey(),
                messageContainer.getOriginatedTime(),
                messageContainer.getPayload(),
                messageContainer.getHeaders());
    }

    protected QueryService getQueryService() {
        return queryService;
    }

    protected SubscriptionId getSubscriptionId() {
        return subscriptionId;
    }

    protected QueueName getQueueName() {
        return queueName;
    }

    protected MessageContextPropagator getMessageContextPropagator() {
        return messageContextPropagator;
    }

    protected MessageConsumerTelemetry getMessageConsumerTelemetry() {
        return messageConsumerTelemetry;
    }

    protected boolean isHistoryEnabled() {
        return historyEnabled;
    }

    protected void pause(Duration duration) {
        pause = duration;
    }
}
