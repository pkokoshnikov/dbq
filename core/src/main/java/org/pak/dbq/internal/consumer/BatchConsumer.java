package org.pak.dbq.internal.consumer;

import lombok.NonNull;
import org.pak.dbq.api.BatchAcknowledger;
import org.pak.dbq.api.BatchMessageHandler;
import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.MessageRecord;
import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.spi.MessageConsumerTelemetry;
import org.pak.dbq.spi.MessageContextPropagator;
import org.pak.dbq.spi.MessageFactory;
import org.pak.dbq.spi.QueryService;
import org.pak.dbq.spi.TransactionService;

import java.math.BigInteger;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    protected void processMessages(List<MessageContainer<T>> messageContainerList) {
        var messageContainersById = new LinkedHashMap<BigInteger, MessageContainer<T>>();
        var records = messageContainerList.stream()
                .map(messageContainer -> {
                    messageContainersById.put(messageContainer.getId(), messageContainer);
                    return new MessageRecord<>(
                            messageContainer.getId(),
                            toMessage(messageContainer),
                            messageContainer.getAttempt(),
                            messageContainer.getExecuteAfter());
                })
                .toList();
        var acknowledgedRecords = new HashSet<BigInteger>();

        batchMessageHandler.handle(records, new RecordingBatchAcknowledger(messageContainersById, acknowledgedRecords));

        if (acknowledgedRecords.size() != messageContainerList.size()) {
            throw new IllegalStateException("Batch handler must acknowledge every message exactly once");
        }
    }

    private class RecordingBatchAcknowledger implements BatchAcknowledger<T> {
        private final Map<BigInteger, MessageContainer<T>> messageContainersById;
        private final Set<BigInteger> acknowledgedRecords;

        private RecordingBatchAcknowledger(
                Map<BigInteger, MessageContainer<T>> messageContainersById,
                Set<BigInteger> acknowledgedRecords
        ) {
            this.messageContainersById = messageContainersById;
            this.acknowledgedRecords = acknowledgedRecords;
        }

        @Override
        public void complete(MessageRecord<T> record) {
            var messageContainer = getPendingRecord(record);
            getQueryService().completeMessage(getSubscriptionId(), messageContainer, isHistoryEnabled());
            acknowledgedRecords.add(record.id());
        }

        @Override
        public void retry(MessageRecord<T> record, Duration duration, Exception exception) {
            if (duration == null || duration.isNegative()) {
                throw new IllegalArgumentException("retry duration must be >= 0");
            }
            if (exception == null) {
                throw new NullPointerException("exception");
            }
            var messageContainer = getPendingRecord(record);
            getQueryService().retryMessage(getSubscriptionId(), messageContainer, duration, exception);
            acknowledgedRecords.add(record.id());
        }

        @Override
        public void fail(MessageRecord<T> record, Exception exception) {
            if (exception == null) {
                throw new NullPointerException("exception");
            }
            var messageContainer = getPendingRecord(record);
            getQueryService().failMessage(getSubscriptionId(), messageContainer, exception, isHistoryEnabled());
            acknowledgedRecords.add(record.id());
        }

        private MessageContainer<T> getPendingRecord(MessageRecord<T> record) {
            if (record == null) {
                throw new NullPointerException("record");
            }
            var messageContainer = messageContainersById.get(record.id());
            if (messageContainer == null) {
                throw new IllegalArgumentException("Record does not belong to current batch");
            }
            if (acknowledgedRecords.contains(record.id())) {
                throw new IllegalStateException("Each batch record can be acknowledged only once");
            }
            return messageContainer;
        }
    }
}
