package org.pak.qdb.internal.persistence;

import lombok.Data;
import lombok.experimental.FieldDefaults;

import java.math.BigInteger;
import java.time.Instant;

@Data
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
public class MessageHistoryContainer<T> {
    BigInteger id;
    T message;
    Integer attempt;
    Instant created;
    Status status;
    String errorMessage;
    String stackTrace;

    public MessageHistoryContainer(
            BigInteger id,
            Integer attempt,
            Instant created,
            Status status,
            T message,
            String errorMessage,
            String stackTrace
    ) {
        this.id = id;
        this.attempt = attempt;
        this.created = created;
        this.status = status;
        this.message = message;
        this.errorMessage = errorMessage;
        this.stackTrace = stackTrace;
    }
}
