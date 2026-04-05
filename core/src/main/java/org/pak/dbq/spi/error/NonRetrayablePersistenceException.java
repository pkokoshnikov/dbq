package org.pak.dbq.spi.error;

public class NonRetrayablePersistenceException extends PersistenceException {
    public NonRetrayablePersistenceException(Throwable cause, Throwable originalCause) {
        super(cause, originalCause);
    }
}
