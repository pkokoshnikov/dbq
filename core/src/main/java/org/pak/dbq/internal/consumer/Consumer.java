package org.pak.dbq.internal.consumer;

import lombok.NonNull;
import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.MessageHandler;
import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.api.policy.BlockingPolicy;
import org.pak.dbq.api.policy.NonRetryablePolicy;
import org.pak.dbq.api.policy.RetryablePolicy;
import org.pak.dbq.error.MessageSerializationException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.spi.MessageConsumerTelemetry;
import org.pak.dbq.spi.MessageContextPropagator;
import org.pak.dbq.spi.MessageFactory;
import org.pak.dbq.spi.QueryService;
import org.pak.dbq.spi.TransactionService;
import org.pak.dbq.spi.error.NonRetrayablePersistenceException;
import org.pak.dbq.spi.error.PersistenceException;
import org.pak.dbq.spi.error.RetryablePersistenceException;
import org.slf4j.MDC;

import java.util.List;
import java.util.Optional;

import static java.util.Optional.of;

public class Consumer<T> extends AbstractConsumer<T> {
    private final MessageHandler<T> messageHandler;
    private final RetryablePolicy retryablePolicy;
    private final NonRetryablePolicy nonRetryablePolicy;
    private final BlockingPolicy blockingPolicy;

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
        super(queueName, subscriptionId, queryService, transactionService, messageContextPropagator,
                messageConsumerTelemetry, messageFactory, properties);
        this.messageHandler = messageHandler;
        this.retryablePolicy = retryablePolicy;
        this.nonRetryablePolicy = nonRetryablePolicy;
        this.blockingPolicy = blockingPolicy;
    }

    @Override
    protected void processMessages(List<MessageContainer<T>> messageContainerList)
            throws PersistenceException, InterruptedException {
        for (var messageContainer : messageContainerList) {
            try (var ignoredMessageContext = getMessageContextPropagator()
                    .extractToCurrentContext(messageContainer.getHeaders());
                    var ignoreExecutorIdMDC = MDC.putCloseable("messageId", messageContainer.getId().toString());
                    var ignoreKeyMDC = MDC.putCloseable("messageKey", messageContainer.getKey())) {
                log.debug("Start message processing");
                var message = toMessage(messageContainer);
                Optional<Exception> optionalException = Optional.empty();
                try (var telemetry = getMessageConsumerTelemetry().start(message, getQueueName(), getSubscriptionId())) {
                    try {
                        messageHandler.handle(message);
                    } catch (MessageSerializationException e) {
                        throw e;
                    } catch (PersistenceException | InterruptedException e) {
                        sneakyThrow(e);
                    } catch (Exception e) {
                        telemetry.recordError(e);
                        optionalException = of(e);
                    }

                    if (optionalException.isEmpty()) {
                        getQueryService().completeMessage(getSubscriptionId(), messageContainer, isHistoryEnabled());
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
    }

    private void handleNonRetryableException(MessageContainer<T> messageContainer, Exception e)
            throws PersistenceException {
        log.error("Non retryable exception occurred", e);
        getQueryService().failMessage(getSubscriptionId(), messageContainer, e, isHistoryEnabled());
    }

    private void handleRetryableException(MessageContainer<T> messageContainer, Exception e)
            throws PersistenceException {
        log.error("Retryable exception occurred, attempt {}", messageContainer.getAttempt(), e);
        var optionalDuration = retryablePolicy.apply(e, messageContainer.getAttempt());

        if (optionalDuration.isEmpty() || messageContainer.getAttempt().equals(Integer.MAX_VALUE - 1)) {
            getQueryService().failMessage(getSubscriptionId(), messageContainer, e, isHistoryEnabled());
        } else {
            getQueryService().retryMessage(getSubscriptionId(), messageContainer, optionalDuration.get(), e);

            log.debug("Task will retry, attempt {} at {}", messageContainer.getAttempt() + 1,
                    messageContainer.getExecuteAfter());
        }
    }

    private void handleBlockingException(Exception e) {
        log.error("Blocking exception occurred", e);
        pause(blockingPolicy.apply(e));
    }
}
