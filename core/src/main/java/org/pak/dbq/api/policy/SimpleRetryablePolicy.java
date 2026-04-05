package org.pak.dbq.api.policy;

import java.time.Duration;
import java.util.Optional;

public class SimpleRetryablePolicy implements RetryablePolicy {

    @Override
    public Optional<Duration> apply(Exception e, Integer attempt) {
        if (attempt == Integer.MAX_VALUE) {
            return Optional.empty();
        } else {
            return Optional.of(Duration.ofSeconds(Math.min((long) Math.pow(2, attempt), 60 * 60)));
        }
    }
}
