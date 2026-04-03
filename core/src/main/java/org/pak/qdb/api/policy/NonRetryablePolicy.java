package org.pak.qdb.api.policy;

public interface NonRetryablePolicy {
    boolean isNonRetryable(Exception exception);
}
