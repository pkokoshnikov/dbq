package org.pak.messagebus.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.messagebus.core.CoreTestSupport.MESSAGE_NAME;

class MessageBusTest {
    @Test
    void registerPublisherAndPublishStoreMessageThroughFacade() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var messageBus = new MessageBus(queryService, transactionService, CronConfig.builder().build());

        messageBus.registerPublisher(PublisherConfig.<String>builder()
                .messageName(MESSAGE_NAME)
                .clazz(String.class)
                .properties(PublisherConfig.Properties.builder().storageDays(10).build())
                .build());

        messageBus.publish("payload");

        assertThat(queryService.inserts).hasSize(1);
        assertThat(queryService.inserts.getFirst().messageName()).isEqualTo(MESSAGE_NAME);
        assertThat(queryService.inserts.getFirst().message().payload()).isEqualTo("payload");
    }
}
