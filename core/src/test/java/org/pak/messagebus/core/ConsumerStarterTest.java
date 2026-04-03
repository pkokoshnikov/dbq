package org.pak.messagebus.core;

import org.junit.jupiter.api.Test;

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
                new CoreTestSupport.RecordingQueryService(),
                new CoreTestSupport.DirectTransactionService(),
                new StdMessageFactory()
        );

        starter.start();
        Thread.sleep(100);
        starter.stop();

        starter.start();
        Thread.sleep(100);
        starter.stop();
    }
}
