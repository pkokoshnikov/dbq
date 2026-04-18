package org.pak.dbq.spi;

import org.pak.dbq.api.Message;
import org.pak.dbq.error.DbqException;

import java.util.List;

public interface ProducerQueryService {
    <T> boolean insertMessage(Message<T> message) throws DbqException;

    <T> List<Boolean> insertBatchMessage(List<Message<T>> messages) throws DbqException;
}
