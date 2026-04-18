package org.pak.dbq.internal.consumer;

import org.junit.jupiter.api.Test;
import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.internal.CoreTestSupport;
import org.pak.dbq.internal.support.SimpleMessageFactory;

class ConsumerStarterTest {
    @Test
    void startAfterStopStartsNewExecutor() throws InterruptedException {
        var starter = new ConsumerStarter<>(
                ConsumerConfig.<String>builder()
                        .queueName(CoreTestSupport.QUEUE_NAME)
                        .subscriptionId(CoreTestSupport.SUBSCRIPTION_NAME)
                        .messageHandler(message -> {})
                        .properties(ConsumerConfig.Properties.builder().build())
                        .build(),
                new CoreTestSupport.RecordingQueryService(
                        CoreTestSupport.QUEUE_NAME,
                        CoreTestSupport.SUBSCRIPTION_NAME,
                        false,
                        false),
                new CoreTestSupport.DirectTransactionService(),
                new SimpleMessageFactory()
        );

        starter.start();
        Thread.sleep(100);
        starter.stop();

        starter.start();
        Thread.sleep(100);
        starter.stop();
    }
}
