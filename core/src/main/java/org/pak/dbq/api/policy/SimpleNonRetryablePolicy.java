package org.pak.dbq.api.policy;

public class SimpleNonRetryablePolicy implements NonRetryablePolicy {

    @Override
    public boolean isNonRetryable(Exception exception) {
        return false;
    }
}
