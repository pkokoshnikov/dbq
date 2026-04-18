package org.pak.dbq.internal.consumer;

import org.junit.jupiter.api.Test;
import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.Message;
import org.pak.dbq.error.DbqException;
import org.pak.dbq.error.MessageDeserializationException;
import org.pak.dbq.internal.CoreTestSupport;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.internal.support.NoOpMessageConsumerTelemetry;
import org.pak.dbq.internal.support.NoOpMessageContextPropagator;
import org.pak.dbq.internal.support.SimpleMessageFactory;
import org.pak.dbq.spi.QueryService;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.dbq.internal.CoreTestSupport.QUEUE_NAME;
import static org.pak.dbq.internal.CoreTestSupport.SUBSCRIPTION_NAME;

class AbstractConsumerPoolLoopTest {
    @Test
    void poolLoopStopsOnMessageDeserializationException() throws Exception {
        var stopLatch = new CountDownLatch(1);
        var consumer = new LoopTestConsumer(new ThrowingQueryService(), stopLatch);
        var thread = new Thread(consumer::poolLoop);

        thread.start();

        assertThat(stopLatch.await(2, TimeUnit.SECONDS)).isTrue();
        thread.join(1000);

        assertThat(thread.isAlive()).isFalse();
    }

    private static final class LoopTestConsumer extends AbstractConsumer<String> {
        private final CountDownLatch stopLatch;

        private LoopTestConsumer(QueryService queryService, CountDownLatch stopLatch) {
            super(
                    QUEUE_NAME,
                    SUBSCRIPTION_NAME,
                    queryService,
                    new CoreTestSupport.DirectTransactionService(),
                    new NoOpMessageContextPropagator(),
                    new NoOpMessageConsumerTelemetry(),
                    new SimpleMessageFactory(),
                    ConsumerConfig.Properties.builder()
                            .persistenceExceptionPause(Duration.ZERO)
                            .unpredictedExceptionPause(Duration.ZERO)
                            .build()
            );
            this.stopLatch = stopLatch;
        }

        @Override
        public void poolLoop() {
            super.poolLoop();
            stopLatch.countDown();
        }

        @Override
        protected void processMessages(List<MessageContainer<String>> messageContainerList) throws DbqException {
        }
    }

    private static final class ThrowingQueryService implements QueryService {
        @Override
        public <T> List<MessageContainer<T>> selectMessages() throws DbqException {
            throw new MessageDeserializationException(new IllegalStateException("broken payload"));
        }

        @Override
        public <T> void retryMessage(MessageContainer<T> messageContainer, Duration retryDuration, Exception e)
                throws DbqException {
        }

        @Override
        public <T> void failMessage(MessageContainer<T> messageContainer, Exception e) throws DbqException {
        }

        @Override
        public <T> void completeMessage(MessageContainer<T> messageContainer) throws DbqException {
        }

        @Override
        public <T> boolean insertMessage(Message<T> message) throws DbqException {
            return true;
        }

        @Override
        public <T> List<Boolean> insertBatchMessage(List<Message<T>> messages) throws DbqException {
            return List.of();
        }
    }
}
