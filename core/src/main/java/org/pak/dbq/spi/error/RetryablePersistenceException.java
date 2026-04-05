package org.pak.dbq.spi.error;

public class RetryablePersistenceException extends PersistenceException {
    public RetryablePersistenceException(Throwable cause, Throwable originalCause) {
        super(cause, originalCause);
    }
}
