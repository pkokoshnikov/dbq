package org.pak.dbq.api;

import java.math.BigInteger;
import java.time.Instant;

public record MessageRecord<T>(
        BigInteger id,
        Message<T> message,
        Integer attempt,
        Instant executeAfter
) {
}
