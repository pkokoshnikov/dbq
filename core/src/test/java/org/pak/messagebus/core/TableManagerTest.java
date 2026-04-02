package org.pak.messagebus.core;

import org.junit.jupiter.api.Test;
import org.quartz.Scheduler;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.messagebus.core.CoreTestSupport.MESSAGE_NAME;
import static org.pak.messagebus.core.CoreTestSupport.SUBSCRIPTION_NAME;

class TableManagerTest {
    @Test
    void registerMessageInitializesTableAndCreatesCurrentAndNextPartitions() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var tableManager = new TableManager(queryService, "* * * * * ?", "* * * * * ?");

        tableManager.registerMessage(MESSAGE_NAME, 30);

        assertThat(queryService.initializedMessageTables).containsExactly(MESSAGE_NAME);
        assertThat(queryService.messagePartitionCreations).hasSize(2);
        assertThat(queryService.messagePartitionCreations)
                .allMatch(call -> call.target().equals(MESSAGE_NAME));
    }

    @Test
    void createPartitionsCreatesNextDayPartitionsForRegisteredEntities() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var tableManager = new TableManager(queryService, "* * * * * ?", "* * * * * ?");
        tableManager.registerMessage(MESSAGE_NAME, 30);
        tableManager.registerSubscription(MESSAGE_NAME, SUBSCRIPTION_NAME, 30);
        queryService.messagePartitionCreations.clear();
        queryService.historyPartitionCreations.clear();

        tableManager.createPartitions();

        assertThat(queryService.messagePartitionCreations).hasSize(1);
        assertThat(queryService.historyPartitionCreations).hasSize(1);
        assertThat(queryService.messagePartitionCreations.getFirst().target()).isEqualTo(MESSAGE_NAME);
        assertThat(queryService.historyPartitionCreations.getFirst().target()).isEqualTo(SUBSCRIPTION_NAME);
        assertThat(queryService.messagePartitionCreations.getFirst().includeDateTime())
                .isAfter(Instant.now().plusSeconds(23 * 60 * 60));
    }

    @Test
    void cleanPartitionsDropsOnlyExpiredPartitionsAndSkipsReferencedOnes() {
        var queryService = new CoreTestSupport.RecordingQueryService();
        var tableManager = new TableManager(queryService, "* * * * * ?", "* * * * * ?");
        tableManager.registerMessage(MESSAGE_NAME, 5);
        tableManager.registerSubscription(MESSAGE_NAME, SUBSCRIPTION_NAME, 5);
        var oldPartition = LocalDate.now(ZoneOffset.UTC).minusDays(10);
        var referencedOldPartition = LocalDate.now(ZoneOffset.UTC).minusDays(9);
        var freshPartition = LocalDate.now(ZoneOffset.UTC).minusDays(2);
        queryService.messagePartitions.put(MESSAGE_NAME, List.of(oldPartition, referencedOldPartition, freshPartition));
        queryService.historyPartitions.put(
                SUBSCRIPTION_NAME,
                List.of(oldPartition, referencedOldPartition, freshPartition)
        );
        queryService.messagePartitionsWithReferences.add(referencedOldPartition);
        queryService.historyPartitionsWithReferences.add(referencedOldPartition);

        tableManager.cleanPartitions();

        assertThat(queryService.droppedMessagePartitions).containsExactly(oldPartition);
        assertThat(queryService.droppedHistoryPartitions).containsExactly(oldPartition);
    }

    @Test
    void stopCronJobsShutsDownScheduler() throws Exception {
        var tableManager = new TableManager(new CoreTestSupport.RecordingQueryService(), "* * * * * ?", "* * * * * ?");

        tableManager.startCronJobs();
        var scheduler = scheduler(tableManager);

        assertThat(scheduler.isShutdown()).isFalse();

        tableManager.stopCronJobs();

        assertThat(scheduler.isShutdown()).isTrue();
    }

    private Scheduler scheduler(TableManager tableManager) throws Exception {
        var schedulerField = TableManager.class.getDeclaredField("scheduler");
        schedulerField.setAccessible(true);
        return (Scheduler) schedulerField.get(tableManager);
    }
}
