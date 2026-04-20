package org.pak.dbq.pg.consumer;

import org.pak.dbq.error.DbqException;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.spi.ConsumerQueryService;

import java.time.Duration;
import java.util.List;

public final class PgConsumerQueryService implements ConsumerQueryService {
    private final ConsumerQueryContext context;
    private final SelectMessagesStrategy selectMessagesStrategy;
    private final RetryMessageStrategy retryMessageStrategy;
    private final FailMessageStrategy failMessageStrategy;
    private final CompleteMessageStrategy completeMessageStrategy;

    public PgConsumerQueryService(
            ConsumerQueryContext context,
            SelectMessagesStrategy selectMessagesStrategy,
            RetryMessageStrategy retryMessageStrategy,
            FailMessageStrategy failMessageStrategy,
            CompleteMessageStrategy completeMessageStrategy
    ) {
        this.context = context;
        this.selectMessagesStrategy = selectMessagesStrategy;
        this.retryMessageStrategy = retryMessageStrategy;
        this.failMessageStrategy = failMessageStrategy;
        this.completeMessageStrategy = completeMessageStrategy;
    }

    @Override
    public <T> List<MessageContainer<T>> selectMessages() throws DbqException {
        return selectMessagesStrategy.selectMessages(context);
    }

    @Override
    public <T> void retryMessage(MessageContainer<T> messageContainer, Duration retryDuration, Exception e)
            throws DbqException {
        retryMessageStrategy.retryMessage(context, messageContainer, retryDuration, e);
    }

    @Override
    public <T> void failMessage(MessageContainer<T> messageContainer, Exception e) throws DbqException {
        failMessageStrategy.failMessage(context, messageContainer, e);
    }

    @Override
    public <T> void completeMessage(MessageContainer<T> messageContainer) throws DbqException {
        completeMessageStrategy.completeMessage(context, messageContainer);
    }
}
