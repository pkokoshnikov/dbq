package org.pak.messagebus.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.messagebus.core.CoreTestSupport.QUEUE_NAME;

class ProducerTest {
    @Test
    void sendCreatesAndInsertsMessageForPayload() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var messageContextPropagator = new CoreTestSupport.RecordingMessageContextPropagator(
                java.util.Map.of("traceparent", "00-test-parent"));
        var producer = new Producer<>(
                ProducerConfig.<String>builder()
                        .queueName(QUEUE_NAME)
                        .clazz(String.class)
                        .properties(ProducerConfig.Properties.builder().storageDays(10).build())
                        .messageContextPropagator(messageContextPropagator)
                        .build(),
                queryService,
                new StdMessageFactory()
        );

        producer.send("payload");

        assertThat(queryService.inserts).hasSize(1);
        var insert = queryService.inserts.getFirst();
        assertThat(insert.queueName()).isEqualTo(QUEUE_NAME);
        assertThat(insert.message().payload()).isEqualTo("payload");
        assertThat(insert.message().key()).isNotBlank();
        assertThat(insert.message().originatedTime()).isNotNull();
        assertThat(insert.message().headers()).containsEntry("traceparent", "00-test-parent");
    }
}
