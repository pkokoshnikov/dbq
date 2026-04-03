package org.pak.qdb.runtime;

import org.junit.jupiter.api.Test;
import org.pak.qdb.api.ProducerConfig;
import org.pak.qdb.api.QueueManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.qdb.runtime.CoreTestSupport.QUEUE_NAME;

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

        assertThat(queryService.inserts).hasSize(1);
        assertThat(queryService.inserts.getFirst().queueName()).isEqualTo(QUEUE_NAME);
        assertThat(queryService.inserts.getFirst().message().payload()).isEqualTo("payload");
    }
}
