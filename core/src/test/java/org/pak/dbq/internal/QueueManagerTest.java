package org.pak.dbq.internal;

import org.junit.jupiter.api.Test;
import org.pak.dbq.api.ProducerConfig;
import org.pak.dbq.api.QueueManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.dbq.internal.CoreTestSupport.QUEUE_NAME;

class QueueManagerTest {
    @Test
    void registerProducerReturnsProducerThatStoresMessage() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var transactionService = new CoreTestSupport.DirectTransactionService();
        var queue = new QueueManager(queryService, transactionService);

        var producer = queue.registerProducer(ProducerConfig.<String>builder()
                .queueName(QUEUE_NAME)
                .clazz(String.class)
                .properties(ProducerConfig.Properties.builder().storageDays(10).build())
                .build());

        producer.send("payload");

        assertThat(queryService.getInserts()).hasSize(1);
        assertThat(queryService.getInserts().getFirst().queueName()).isEqualTo(QUEUE_NAME);
        assertThat(queryService.getInserts().getFirst().message().payload()).isEqualTo("payload");
    }
}
