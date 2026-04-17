package org.pak.dbq.api;

import java.util.List;

public interface BatchMessageHandler<T> {
    void handle(List<MessageRecord<T>> messages, BatchAcknowledger<T> acknowledger);
}
