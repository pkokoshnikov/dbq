package org.pak.messagebus.core;

public class SimpleNonRetryablePolicy implements NonRetryablePolicy {

    @Override
    public boolean isNonRetryable(Exception exception) {
        return false;
    }
}
