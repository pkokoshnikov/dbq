package org.pak.qdb.internal.persistence;

import lombok.Data;
import lombok.experimental.FieldDefaults;

import java.math.BigInteger;
import java.time.Instant;
import java.util.Map;

@Data
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
public class MessageContainer<T> {
    BigInteger id;
    BigInteger messageId;
    String key;
    T payload;
    Map<String, String> headers;
    Integer attempt;
    Instant executeAfter;
    Instant created;
    Instant updated;
    Instant originatedTime;
    String errorMessage;
    String stackTrace;

    public MessageContainer(
            BigInteger id,
            BigInteger messageId,
            String key,
            Integer attempt,
            Instant executeAfter,
            Instant created,
            Instant updated,
            Instant originatedTime,
            T payload,
            Map<String, String> headers,
            String errorMessage,
            String stackTrace
    ) {
        this.id = id;
        this.messageId = messageId;
        this.key = key;
        this.attempt = attempt;
        this.executeAfter = executeAfter;
        this.created = created;
        this.updated = updated;
        this.originatedTime = originatedTime;
        this.payload = payload;
        this.headers = Map.copyOf(headers);
        this.errorMessage = errorMessage;
        this.stackTrace = stackTrace;
    }
}
