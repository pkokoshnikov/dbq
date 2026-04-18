package org.pak.dbq.error;

public class RetryablePersistenceException extends RetryableDbqException {
    public RetryablePersistenceException(Throwable cause, Throwable originalCause) {
        super(cause, originalCause);
    }
}
