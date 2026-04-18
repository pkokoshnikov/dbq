package org.pak.dbq.error;

public class MessageSerializationException extends NonRetryableDbqException {
    public MessageSerializationException(Throwable throwable) {
        super(throwable, throwable.getCause());
    }
}
