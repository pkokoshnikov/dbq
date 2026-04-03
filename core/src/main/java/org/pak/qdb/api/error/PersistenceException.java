package org.pak.qdb.api.error;

import lombok.Getter;

public class PersistenceException extends RuntimeException {
    @Getter
    private final Throwable originalCause;

    public PersistenceException(Throwable cause, Throwable originalCause) {
        super(cause);
        this.originalCause = originalCause;
    }
}
