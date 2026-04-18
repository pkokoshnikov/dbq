package org.pak.dbq.error;

public class NonRetrayablePersistenceException extends NonRetryableDbqException {
    public NonRetrayablePersistenceException(Throwable cause, Throwable originalCause) {
        super(cause, originalCause);
    }
}
