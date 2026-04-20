package org.pak.dbq.pg.consumer;

import org.pak.dbq.internal.persistence.MessageContainer;

import java.math.BigInteger;
import java.time.Instant;
import java.util.Map;

final class TestMessageContainers {
    private TestMessageContainers() {
    }

    static MessageContainer<String> message() {
        return new MessageContainer<>(
                BigInteger.ONE,
                BigInteger.ONE,
                "test-key",
                0,
                Instant.parse("2026-04-20T10:15:30Z"),
                Instant.parse("2026-04-20T10:15:30Z"),
                Instant.parse("2026-04-20T10:15:30Z"),
                Instant.parse("2026-04-20T10:15:30Z"),
                "payload",
                Map.of(),
                null,
                null);
    }
}
