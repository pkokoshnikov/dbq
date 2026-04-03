package org.pak.qdb.api.error;

public class NonRetrayablePersistenceException extends PersistenceException {
    public NonRetrayablePersistenceException(Throwable cause, Throwable originalCause) {
        super(cause, originalCause);
    }
}
