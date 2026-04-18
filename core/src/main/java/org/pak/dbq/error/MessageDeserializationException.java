package org.pak.dbq.error;

public class MessageDeserializationException extends NonRetryableDbqException {
    public MessageDeserializationException(Throwable throwable) {
        super(throwable, throwable.getCause());
    }
}
