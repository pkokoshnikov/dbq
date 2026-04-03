package org.pak.qdb.policy;

import java.time.Duration;
import java.util.Optional;

public interface RetryablePolicy {
    Optional<Duration> apply(Exception e, Integer attempt);
}
