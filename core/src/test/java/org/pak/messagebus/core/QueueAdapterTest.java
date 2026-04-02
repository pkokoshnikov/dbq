package org.pak.messagebus.core;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class QueueAdapterTest {
    @Test
    void startSubscribersCanBeCalledWithoutPartitionManager() {
        var queueAdapter = new QueueAdapter(
                new CoreTestSupport.RecordingQueryService(),
                new CoreTestSupport.DirectTransactionService(),
                new StdMessageFactory()
        );

        queueAdapter.startSubscribers();
        queueAdapter.stopSubscribers();
    }

    @Test
    void registerPublisherAndPublishDelegatesBatchInsert() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var queueAdapter = new QueueAdapter(
                queryService,
                new CoreTestSupport.DirectTransactionService(),
                new StdMessageFactory()
        );

        queueAdapter.registerPublisher(PublisherConfig.<String>builder()
                .messageName(CoreTestSupport.MESSAGE_NAME)
                .clazz(String.class)
                .properties(PublisherConfig.Properties.builder().storageDays(10).build())
                .build());

        var message = new StdMessage<>("key-1", Instant.now(), "payload");
        queueAdapter.publish(String.class, List.of(message));

        assertThat(queryService.batchInserts).hasSize(1);
        assertThat(queryService.batchInserts.getFirst().messageName()).isEqualTo(CoreTestSupport.MESSAGE_NAME);
        assertThat(queryService.batchInserts.getFirst().messages()).hasSize(1);
        assertThat(queryService.batchInserts.getFirst().messages().getFirst()).isEqualTo(message);
    }
}
