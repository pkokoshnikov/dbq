package org.pak.dbq.error;

public class NonRetryablePersistenceException extends NonRetryableDbqException {
    public NonRetryablePersistenceException(Throwable cause, Throwable originalCause) {
        super(cause, originalCause);
    }
}
