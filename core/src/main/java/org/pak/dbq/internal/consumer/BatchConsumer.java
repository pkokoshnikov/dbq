package org.pak.dbq.internal.consumer;

import lombok.NonNull;
import org.pak.dbq.api.*;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.spi.*;
import org.pak.dbq.spi.error.PersistenceException;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;

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
            throws PersistenceException, InterruptedException {
        var messageContainersById = new LinkedHashMap<BigInteger, MessageContainer<T>>();
        var records = messageContainerList.stream()
                .map(messageContainer -> {
                    messageContainersById.put(messageContainer.getId(), messageContainer); // todo: я бы вынес это просто в отдельную операцию на стримах
                    return new MessageRecord<>(
                            messageContainer.getId(),
                            toMessage(messageContainer),
                            messageContainer.getAttempt(),
                            messageContainer.getExecuteAfter());
                })
                .toList();
        var acknowledgedRecords = new HashSet<BigInteger>();

        batchMessageHandler.handle(records, new RecordingAcknowledger<>(
                getQueryService(),
                getSubscriptionId(),
                isHistoryEnabled(),
                messageContainersById,
                acknowledgedRecords));

        if (acknowledgedRecords.size() != messageContainerList.size()) {
            throw new IllegalStateException("Batch handler must acknowledge every message exactly once");
        }
    }
}
