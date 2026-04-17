package org.pak.dbq.spi.error;

import lombok.Getter;

public class PersistenceException extends Exception {
    @Getter
    private final Throwable originalCause;

    public PersistenceException(Throwable cause, Throwable originalCause) {
        super(cause);
        this.originalCause = originalCause;
    }
}
