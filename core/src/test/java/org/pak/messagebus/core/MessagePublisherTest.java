package org.pak.messagebus.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.messagebus.core.CoreTestSupport.MESSAGE_NAME;

class MessagePublisherTest {
    @Test
    void publishCreatesAndInsertsMessageForPayload() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var publisher = new MessagePublisher<>(
                PublisherConfig.<String>builder()
                        .messageName(MESSAGE_NAME)
                        .clazz(String.class)
                        .properties(PublisherConfig.Properties.builder().storageDays(10).build())
                        .build(),
                queryService,
                new StdMessageFactory()
        );

        publisher.publish("payload");

        assertThat(queryService.inserts).hasSize(1);
        var insert = queryService.inserts.getFirst();
        assertThat(insert.messageName()).isEqualTo(MESSAGE_NAME);
        assertThat(insert.message().payload()).isEqualTo("payload");
        assertThat(insert.message().key()).isNotBlank();
        assertThat(insert.message().originatedTime()).isNotNull();
    }
}
