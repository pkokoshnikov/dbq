package org.pak.dbq.internal.consumer;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.MessageHandler;
import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.spi.error.NonRetrayablePersistenceException;
import org.pak.dbq.spi.error.RetryablePersistenceException;
import org.pak.dbq.error.MessageSerializationException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.api.policy.BlockingPolicy;
import org.pak.dbq.api.policy.NonRetryablePolicy;
import org.pak.dbq.api.policy.RetryablePolicy;
import org.pak.dbq.spi.*;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Optional.of;

@Slf4j
public class Consumer<T> {
    private final String id = UUID.randomUUID().toString();
    private final RetryablePolicy retryablePolicy;
    private final NonRetryablePolicy nonRetryablePolicy;
    private final BlockingPolicy blockingPolicy;
    private final QueryService queryService;
    private final QueueName queueName;
    private final SubscriptionId subscriptionId;
    private final TransactionService transactionService;
    private final MessageContextPropagator messageContextPropagator;
    private final MessageConsumerTelemetry messageConsumerTelemetry;
    private final MessageHandler<T> messageHandler;
    private Duration pause = null;
    private final Duration unpredictedExceptionPause;
    private final MessageFactory messageFactory;
    private final Integer maxPollRecords;
    private final Duration persistenceExceptionPause;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    public Consumer(
            @NonNull MessageHandler<T> messageHandler,
            @NonNull QueueName queueName,
            @NonNull SubscriptionId subscriptionId,
            @NonNull RetryablePolicy retryablePolicy,
            @NonNull NonRetryablePolicy nonRetryablePolicy,
            @NonNull BlockingPolicy blockingPolicy,
            @NonNull QueryService queryService,
            @NonNull TransactionService transactionService,
            @NonNull MessageContextPropagator messageContextPropagator,
            @NonNull MessageConsumerTelemetry messageConsumerTelemetry,
            @NonNull MessageFactory messageFactory,
            @NonNull ConsumerConfig.Properties properties
    ) {
        this.messageHandler = messageHandler;
        this.retryablePolicy = retryablePolicy;
        this.nonRetryablePolicy = nonRetryablePolicy;
        this.blockingPolicy = blockingPolicy;
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
                } catch (MessageSerializationException e) { /*service layer exceptions*/
                    log.error("Serializer exception occurred, we cannot skip messages", e);
                    isRunning.set(false);
                } catch (NonRetrayablePersistenceException e) {
                    log.error("Non recoverable persistence exception occurred, stop processing", e);
                    isRunning.set(false);
                } catch (RetryablePersistenceException e) {
                    log.error("Recoverable persistence exception occurred", e);
                    pause = persistenceExceptionPause;
                } catch (InterruptedException e) {  /*app layer exceptions*/
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

    boolean poolAndProcess() {
        List<MessageContainer<T>> messageContainerList =
                queryService.selectMessages(queueName, subscriptionId, maxPollRecords);

        if (messageContainerList.size() == 0) {
            return false;
        }

        for (var messageContainer : messageContainerList) {
            try (var ignoredMessageContext = messageContextPropagator.extractToCurrentContext(messageContainer.getHeaders());
                    var ignoreExecutorIdMDC = MDC.putCloseable("messageId", messageContainer.getId().toString());
                    var ignoreKeyMDC = MDC.putCloseable("messageKey", messageContainer.getKey())) {
                log.debug("Start message processing");
                var message = messageFactory.createMessage(messageContainer.getKey(),
                        messageContainer.getOriginatedTime(),
                        messageContainer.getPayload(),
                        messageContainer.getHeaders());
                Optional<Exception> optionalException = Optional.empty();
                try (var ignoredConsumerTelemetry =
                             messageConsumerTelemetry.start(message, queueName, subscriptionId)) {
                    try {
                        messageHandler.handle(message);
                    } catch (Exception e) {
                        ignoredConsumerTelemetry.recordError(e);
                        optionalException = of(e);
                    }

                    if (optionalException.isEmpty()) {
                        queryService.completeMessage(subscriptionId, messageContainer);
                        log.info("Message processing completed");
                    } else {
                        var exception = optionalException.get();
                        if (blockingPolicy.isBlocked(exception)) {
                            handleBlockingException(exception);
                        } else if (nonRetryablePolicy.isNonRetryable(exception)) {
                            handleNonRetryableException(messageContainer, exception);
                        } else {
                            handleRetryableException(messageContainer, exception);
                        }
                    }
                }
            }
        }

        return true;
    }

    private void handleNonRetryableException(MessageContainer<T> messageContainer, Exception e) {
        log.error("Non retryable exception occurred", e);
        queryService.failMessage(subscriptionId, messageContainer, e);
    }

    private void handleRetryableException(MessageContainer<T> messageContainer, Exception e) {
        log.error("Retryable exception occurred, attempt {}", messageContainer.getAttempt(), e);
        var optionalDuration = retryablePolicy.apply(e, messageContainer.getAttempt());

        if (optionalDuration.isEmpty() || messageContainer.getAttempt().equals(Integer.MAX_VALUE - 1)) {
            queryService.failMessage(subscriptionId, messageContainer, e);
        } else {
            queryService.retryMessage(subscriptionId, messageContainer, optionalDuration.get(), e);

            log.debug("Task will retry, attempt {} at {}", messageContainer.getAttempt() + 1,
                    messageContainer.getExecuteAfter());
        }
    }

    private void handleBlockingException(Exception e) {
        log.error("Blocking exception occurred", e);
        pause = blockingPolicy.apply(e);
    }

    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            log.info("Prepare to stop consumer");
        } else {
            log.warn("Consumer should be stopped only once");
        }
    }
}
