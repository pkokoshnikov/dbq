package org.pak.messagebus.core;

import org.junit.jupiter.api.Test;

class MessageProcessorStarterTest {
    @Test
    void startAfterStopStartsNewExecutor() throws InterruptedException {
        var starter = new MessageProcessorStarter<>(
                SubscriberConfig.<String>builder()
                        .messageName(CoreTestSupport.MESSAGE_NAME)
                        .subscriptionName(CoreTestSupport.SUBSCRIPTION_NAME)
                        .messageListener(message -> {})
                        .properties(SubscriberConfig.Properties.builder().build())
                        .build(),
                new CoreTestSupport.RecordingQueryService(),
                new CoreTestSupport.DirectTransactionService(),
                new StdMessageFactory(),
                new TableManager(new CoreTestSupport.RecordingQueryService(), "* * * * * ?", "* * * * * ?")
        );

        starter.start();
        Thread.sleep(100);
        starter.stop();

        starter.start();
        Thread.sleep(100);
        starter.stop();
    }
}
