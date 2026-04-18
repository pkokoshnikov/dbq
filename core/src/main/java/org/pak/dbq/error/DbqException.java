package org.pak.dbq.error;

import lombok.Getter;

public abstract class DbqException extends Exception {
    @Getter
    private final Throwable originalCause;

    protected DbqException(Throwable cause, Throwable originalCause) {
        super(cause);
        this.originalCause = originalCause;
    }
}
