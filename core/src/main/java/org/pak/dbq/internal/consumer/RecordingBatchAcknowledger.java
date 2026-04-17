package org.pak.dbq.internal.consumer;

import org.pak.dbq.api.BatchAcknowledger;
import org.pak.dbq.api.MessageRecord;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.spi.QueryService;

import java.math.BigInteger;
import java.time.Duration;
import java.util.Map;
import java.util.Set;

final class RecordingBatchAcknowledger<T> implements BatchAcknowledger<T> {
    private final QueryService queryService;
    private final SubscriptionId subscriptionId;
    private final boolean historyEnabled;
    private final Map<BigInteger, MessageContainer<T>> messageContainersById;
    private final Set<BigInteger> acknowledgedRecords;

    RecordingBatchAcknowledger(
            QueryService queryService,
            SubscriptionId subscriptionId,
            boolean historyEnabled,
            Map<BigInteger, MessageContainer<T>> messageContainersById,
            Set<BigInteger> acknowledgedRecords
    ) {
        this.queryService = queryService;
        this.subscriptionId = subscriptionId;
        this.historyEnabled = historyEnabled;
        this.messageContainersById = messageContainersById;
        this.acknowledgedRecords = acknowledgedRecords;
    }

    @Override
    public void complete(MessageRecord<T> record) {
        var messageContainer = getPendingRecord(record);
        queryService.completeMessage(subscriptionId, messageContainer, historyEnabled);
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
        queryService.retryMessage(subscriptionId, messageContainer, duration, exception);
        acknowledgedRecords.add(record.id());
    }

    @Override
    public void fail(MessageRecord<T> record, Exception exception) {
        if (exception == null) {
            throw new NullPointerException("exception");
        }
        var messageContainer = getPendingRecord(record);
        queryService.failMessage(subscriptionId, messageContainer, exception, historyEnabled);
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
