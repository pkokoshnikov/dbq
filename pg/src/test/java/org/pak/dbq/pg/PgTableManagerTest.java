package org.pak.dbq.pg;

import org.junit.jupiter.api.Test;
import org.pak.dbq.error.RetryablePersistenceException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class PgTableManagerTest {
    @Test
    void doJobRetriesAfterRetryablePersistenceException() throws Exception {
        var attempts = new AtomicInteger();
        var sleptDurations = new ArrayList<Duration>();

        PgTableManager.doJob(
                null,
                ignored -> {
                    if (attempts.getAndIncrement() == 0) {
                        throw new RetryablePersistenceException(new RuntimeException("db"), null);
                    }
                },
                Duration.ofMillis(1),
                sleptDurations::add
        );

        assertThat(attempts).hasValue(2);
        assertThat(sleptDurations).containsExactly(Duration.ofMillis(1));
    }
}
