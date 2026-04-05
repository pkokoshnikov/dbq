package org.pak.dbq.error;

public class MessageSerializationException extends RuntimeException {
    public MessageSerializationException(Throwable throwable) {
        super(throwable);
    }
}
