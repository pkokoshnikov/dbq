package org.pak.qdb.policy;

public class SimpleNonRetryablePolicy implements NonRetryablePolicy {

    @Override
    public boolean isNonRetryable(Exception exception) {
        return false;
    }
}
