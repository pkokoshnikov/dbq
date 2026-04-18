package org.pak.dbq.error;

public class NonRetryableDbqException extends DbqException {
    public NonRetryableDbqException(Throwable cause, Throwable originalCause) {
        super(cause, originalCause);
    }
}
