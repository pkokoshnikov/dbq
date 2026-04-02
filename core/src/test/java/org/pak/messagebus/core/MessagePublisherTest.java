package org.pak.messagebus.core;

import org.junit.jupiter.api.Test;
import org.pak.messagebus.core.error.MissingPartitionException;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.messagebus.core.CoreTestSupport.MESSAGE_NAME;

class MessagePublisherTest {
    @Test
    void publishCreatesAndInsertsMessageForPayload() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var tableManager = new TableManager(queryService, "* * * * * ?", "* * * * * ?");
        var publisher = new MessagePublisher<>(
                PublisherConfig.<String>builder()
                        .messageName(MESSAGE_NAME)
                        .clazz(String.class)
                        .properties(PublisherConfig.Properties.builder().storageDays(10).build())
                        .build(),
                queryService,
                new StdMessageFactory(),
                tableManager
        );

        publisher.publish("payload");

        assertThat(queryService.inserts).hasSize(1);
        var insert = queryService.inserts.getFirst();
        assertThat(insert.messageName()).isEqualTo(MESSAGE_NAME);
        assertThat(insert.message().payload()).isEqualTo("payload");
        assertThat(insert.message().key()).isNotBlank();
        assertThat(insert.message().originatedTime()).isNotNull();
    }

    @Test
    void publishRetriesAfterMissingPartition() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var originatedTime = Instant.parse("2026-04-02T10:15:30Z");
        queryService.insertMessageResults.add(new MissingPartitionException(List.of(originatedTime)));
        queryService.insertMessageResults.add(Boolean.TRUE);
        var tableManager = new TableManager(queryService, "* * * * * ?", "* * * * * ?");
        var publisher = new MessagePublisher<>(
                PublisherConfig.<String>builder()
                        .messageName(MESSAGE_NAME)
                        .clazz(String.class)
                        .properties(PublisherConfig.Properties.builder().storageDays(10).build())
                        .build(),
                queryService,
                new StdMessageFactory(),
                tableManager
        );

        publisher.publish(new StdMessage<>("key-1", originatedTime, "payload"));

        assertThat(queryService.inserts).hasSize(2);
        assertThat(queryService.messagePartitionCreations)
                .anyMatch(call -> call.target().equals(MESSAGE_NAME) && call.includeDateTime().equals(originatedTime));
    }
}
