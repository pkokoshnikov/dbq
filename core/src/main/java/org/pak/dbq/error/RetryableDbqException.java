package org.pak.dbq.error;

public class RetryableDbqException extends DbqException {
    public RetryableDbqException(Throwable cause, Throwable originalCause) {
        super(cause, originalCause);
    }
}
