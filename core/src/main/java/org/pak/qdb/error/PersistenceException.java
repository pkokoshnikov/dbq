package org.pak.qdb.error;

import lombok.Getter;

public class PersistenceException extends RuntimeException {
    @Getter
    private final Throwable originalCause;

    public PersistenceException(Throwable cause, Throwable originalCause) {
        super(cause);
        this.originalCause = originalCause;
    }
}
