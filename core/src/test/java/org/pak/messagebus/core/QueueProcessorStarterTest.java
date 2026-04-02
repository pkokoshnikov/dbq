package org.pak.messagebus.core;

import org.junit.jupiter.api.Test;

class QueueProcessorStarterTest {
    @Test
    void startAfterStopStartsNewExecutor() throws InterruptedException {
        var starter = new QueueProcessorStarter<>(
                ConsumerConfig.<String>builder()
                        .queueName(CoreTestSupport.QUEUE_NAME)
                        .subscriptionName(CoreTestSupport.SUBSCRIPTION_NAME)
                        .consumer(message -> {})
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
