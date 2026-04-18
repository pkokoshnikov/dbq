package org.pak.dbq.internal.consumer;

import lombok.NonNull;
import org.pak.dbq.api.*;
import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.spi.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class BatchConsumer<T> extends AbstractConsumer<T> {
    private final BatchMessageHandler<T> batchMessageHandler;

    public BatchConsumer(
            @NonNull BatchMessageHandler<T> batchMessageHandler,
            @NonNull QueueName queueName,
            @NonNull SubscriptionId subscriptionId,
            @NonNull QueryService queryService,
            @NonNull TransactionService transactionService,
            @NonNull MessageContextPropagator messageContextPropagator,
            @NonNull MessageConsumerTelemetry messageConsumerTelemetry,
            @NonNull MessageFactory messageFactory,
            @NonNull ConsumerConfig.Properties properties
    ) {
        super(queueName, subscriptionId, queryService, transactionService, messageContextPropagator,
                messageConsumerTelemetry, messageFactory, properties);
        this.batchMessageHandler = batchMessageHandler;
    }

    @Override
    protected void processMessages(List<MessageContainer<T>> messageContainerList)
            throws DbqException, InterruptedException {
        var messageContainersById = messageContainerList.stream()
                .collect(Collectors.toMap(
                        MessageContainer::getId,
                        Function.identity(),
                        (left, right) -> left,
                        LinkedHashMap::new));
        var records = messageContainerList.stream()
                .map(messageContainer -> new MessageRecord<>(
                        messageContainer.getId(),
                        toMessage(messageContainer),
                        messageContainer.getAttempt(),
                        messageContainer.getExecuteAfter()))
                .toList();
        var acknowledger = new RecordingAcknowledger<>(
                getQueryService(),
                messageContainersById);

        batchMessageHandler.handle(records, acknowledger);

        if (acknowledger.acknowledgedCount() != messageContainerList.size()) {
            throw new IllegalStateException("Batch handler must acknowledge every message exactly once");
        }
    }
}
