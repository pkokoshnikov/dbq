package org.pak.dbq.api.policy;

public interface NonRetryablePolicy {
    boolean isNonRetryable(Exception exception);
}
