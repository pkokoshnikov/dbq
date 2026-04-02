package org.pak.messagebus.core;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.pak.messagebus.core.error.NonRetrayablePersistenceException;
import org.pak.messagebus.core.error.RetrayablePersistenceException;
import org.pak.messagebus.core.error.SerializerException;
import org.pak.messagebus.core.service.QueryService;
import org.pak.messagebus.core.service.TransactionService;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Optional.of;
import static java.util.Optional.ofNullable;

@Slf4j
class MessageProcessor<T> {
    private final String id = UUID.randomUUID().toString();
    private final RetryablePolicy retryablePolicy;
    private final NonRetryablePolicy nonRetryablePolicy;
    private final BlockingPolicy blockingPolicy;
    private final QueryService queryService;
    private final MessageName messageName;
    private final SubscriptionName subscriptionName;
    private final TransactionService transactionService;
    private final TraceIdExtractor<T> traceIdExtractor;
    private final MessageListener<T> messageListener;
    private Duration pause = null;
    private final Duration unpredictedExceptionPause;
    private final MessageFactory messageFactory;
    private final Integer maxPollRecords;
    private final Duration persistenceExceptionPause;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    MessageProcessor(
            @NonNull MessageListener<T> messageListener,
            @NonNull MessageName messageName,
            @NonNull SubscriptionName subscriptionName,
            @NonNull RetryablePolicy retryablePolicy,
            @NonNull NonRetryablePolicy nonRetryablePolicy,
            @NonNull BlockingPolicy blockingPolicy,
            @NonNull QueryService queryService,
            @NonNull TransactionService transactionService,
            @NonNull TraceIdExtractor<T> traceIdExtractor,
            @NonNull MessageFactory messageFactory,
            @NonNull SubscriberConfig.Properties properties
    ) {
        this.messageListener = messageListener;
        this.retryablePolicy = retryablePolicy;
        this.nonRetryablePolicy = nonRetryablePolicy;
        this.blockingPolicy = blockingPolicy;
        this.queryService = queryService;
        this.messageName = messageName;
        this.subscriptionName = subscriptionName;
        this.transactionService = transactionService;
        this.traceIdExtractor = traceIdExtractor;
        this.messageFactory = messageFactory;
        this.maxPollRecords = properties.getMaxPollRecords();
        this.persistenceExceptionPause = properties.getPersistenceExceptionPause();
        this.unpredictedExceptionPause = properties.getUnpredictedExceptionPause();
    }

    public void poolLoop() {
        if (!isRunning.compareAndSet(false, true)) {
            log.warn("Event processor should be started only once");
            return;
        }

        try (var ignoreExecutorIdMDC = MDC.putCloseable("messageProcessorId", id);
                var ignoredEventNameMDC = MDC.putCloseable("messageName", messageName.name());
                var ignoredSubscriptionMDC = MDC.putCloseable("subscriptionName", subscriptionName.name())) {
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
                } catch (SerializerException e) { /*service layer exceptions*/
                    log.error("Serializer exception occurred, we cannot skip messages", e);
                    isRunning.set(false);
                } catch (NonRetrayablePersistenceException e) {
                    log.error("Non recoverable persistence exception occurred, stop processing", e);
                    isRunning.set(false);
                } catch (RetrayablePersistenceException e) {
                    log.error("Recoverable persistence exception occurred", e);
                    pause = persistenceExceptionPause;
                } catch (InterruptedException e) {  /*app layer exceptions*/
                    log.warn("Message processor is interrupted", e);

                    Thread.currentThread().interrupt();
                    isRunning.set(false);
                } catch (Exception e) {
                    log.error("Unexpected exception occurred", e);
                    pause = unpredictedExceptionPause;
                }
            } while (isRunning.get());

            log.info("Event processor is stopped");
        }

    }

    boolean poolAndProcess() {
        List<MessageContainer<T>> messageContainerList =
                queryService.selectMessages(messageName, subscriptionName, maxPollRecords);

        if (messageContainerList.size() == 0) {
            return false;
        }

        for (var messageContainer : messageContainerList) {
            var optionalTraceIdMDC = ofNullable(traceIdExtractor.extractTraceId(messageContainer.getPayload()))
                    .map(v -> MDC.putCloseable("traceId", v));
            try (var ignoreExecutorIdMDC = MDC.putCloseable("messageId", messageContainer.getId().toString());
                    var ignoreKeyMDC = MDC.putCloseable("messageKey", messageContainer.getKey())) {
                log.debug("Start message processing");
                Optional<Exception> optionalException = Optional.empty();
                try {
                    messageListener.handle(messageFactory.createMessage(messageContainer.getKey(),
                            messageContainer.getOriginatedTime(),
                            messageContainer.getPayload()));
                } catch (Exception e) {
                    optionalException = of(e);
                }

                if (optionalException.isEmpty()) {
                    queryService.completeMessage(subscriptionName, messageContainer);
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
            } finally {
                optionalTraceIdMDC.ifPresent(MDC.MDCCloseable::close);
            }
        }

        return true;
    }

    private void handleNonRetryableException(MessageContainer<T> messageContainer, Exception e) {
        log.error("Non retryable exception occurred", e);
        queryService.failMessage(subscriptionName, messageContainer, e);
    }

    private void handleRetryableException(MessageContainer<T> messageContainer, Exception e) {
        log.error("Retryable exception occurred, attempt {}", messageContainer.getAttempt(), e);
        var optionalDuration = retryablePolicy.apply(e, messageContainer.getAttempt());

        if (optionalDuration.isEmpty() || messageContainer.getAttempt().equals(Integer.MAX_VALUE - 1)) {
            queryService.failMessage(subscriptionName, messageContainer, e);
        } else {
            queryService.retryMessage(subscriptionName, messageContainer, optionalDuration.get(), e);

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
            log.info("Prepare to stop payload processor");
        } else {
            log.warn("Event processor should be stopped only once");
        }
    }
}
