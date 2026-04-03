package org.pak.qdb.error;

public class NonRetrayablePersistenceException extends PersistenceException {
    public NonRetrayablePersistenceException(Throwable cause, Throwable originalCause) {
        super(cause, originalCause);
    }
}
