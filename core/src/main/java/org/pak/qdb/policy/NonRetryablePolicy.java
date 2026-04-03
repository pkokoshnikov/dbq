package org.pak.qdb.policy;

public interface NonRetryablePolicy {
    boolean isNonRetryable(Exception exception);
}
