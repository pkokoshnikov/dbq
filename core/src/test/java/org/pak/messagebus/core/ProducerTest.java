package org.pak.messagebus.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.messagebus.core.CoreTestSupport.QUEUE_NAME;

class ProducerTest {
    @Test
    void publishCreatesAndInsertsMessageForPayload() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var producer = new Producer<>(
                ProducerConfig.<String>builder()
                        .queueName(QUEUE_NAME)
                        .clazz(String.class)
                        .properties(ProducerConfig.Properties.builder().storageDays(10).build())
                        .build(),
                queryService,
                new StdMessageFactory()
        );

        producer.publish("payload");

        assertThat(queryService.inserts).hasSize(1);
        var insert = queryService.inserts.getFirst();
        assertThat(insert.queueName()).isEqualTo(QUEUE_NAME);
        assertThat(insert.message().payload()).isEqualTo("payload");
        assertThat(insert.message().key()).isNotBlank();
        assertThat(insert.message().originatedTime()).isNotNull();
    }
}
