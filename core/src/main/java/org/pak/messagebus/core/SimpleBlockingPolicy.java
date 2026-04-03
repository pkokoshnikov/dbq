package org.pak.messagebus.core;

import lombok.NonNull;

import java.time.Duration;
import java.time.temporal.ChronoUnit;


public class SimpleBlockingPolicy implements BlockingPolicy {

    @Override
    public boolean isBlocked(Exception exception) {
        return false;
    }

    @Override
    @NonNull
    public Duration apply(Exception exception) {
        return Duration.of(0, ChronoUnit.SECONDS);
    }
}
