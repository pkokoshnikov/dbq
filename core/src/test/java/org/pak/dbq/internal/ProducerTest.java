package org.pak.dbq.internal;

import org.junit.jupiter.api.Test;
import org.pak.dbq.api.Producer;
import org.pak.dbq.api.ProducerConfig;
import org.pak.dbq.internal.support.SimpleMessageFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.dbq.internal.CoreTestSupport.QUEUE_NAME;

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
                        .properties(ProducerConfig.Properties.builder().retentionDays(10).build())
                        .messageContextPropagator(messageContextPropagator)
                        .build(),
                queryService,
                new SimpleMessageFactory()
        );

        producer.send("payload");

        assertThat(queryService.getInserts()).hasSize(1);
        var insert = queryService.getInserts().getFirst();
        assertThat(insert.queueName()).isEqualTo(QUEUE_NAME);
        assertThat(insert.message().payload()).isEqualTo("payload");
        assertThat(insert.message().key()).isNotBlank();
        assertThat(insert.message().originatedTime()).isNotNull();
        assertThat(insert.message().headers()).containsEntry("traceparent", "00-test-parent");
    }
}
