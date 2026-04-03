package org.pak.qdb.api.policy;

import java.time.Duration;
import java.util.Optional;

public interface RetryablePolicy {
    Optional<Duration> apply(Exception e, Integer attempt);
}
