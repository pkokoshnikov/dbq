package org.pak.messagebus.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class QueueAdapterTest {
    @Test
    void startSubscribersStartsTableManagerCronJobs() {
        var tableManager = new RecordingTableManager();
        var queueAdapter = new QueueAdapter(
                new CoreTestSupport.RecordingQueryService(),
                new CoreTestSupport.DirectTransactionService(),
                new StdMessageFactory(),
                tableManager
        );

        queueAdapter.startSubscribers();

        assertThat(tableManager.startCronJobsCalls).isEqualTo(1);
        assertThat(tableManager.stopCronJobsCalls).isZero();
    }

    @Test
    void stopSubscribersStopsTableManagerCronJobs() {
        var tableManager = new RecordingTableManager();
        var queueAdapter = new QueueAdapter(
                new CoreTestSupport.RecordingQueryService(),
                new CoreTestSupport.DirectTransactionService(),
                new StdMessageFactory(),
                tableManager
        );

        queueAdapter.startSubscribers();
        queueAdapter.stopSubscribers();

        assertThat(tableManager.startCronJobsCalls).isEqualTo(1);
        assertThat(tableManager.stopCronJobsCalls).isEqualTo(1);
    }

    private static class RecordingTableManager extends TableManager {
        private int startCronJobsCalls;
        private int stopCronJobsCalls;

        private RecordingTableManager() {
            super(new CoreTestSupport.RecordingQueryService(), "* * * * * ?", "* * * * * ?");
        }

        @Override
        void startCronJobs() {
            startCronJobsCalls++;
        }

        @Override
        void stopCronJobs() {
            stopCronJobsCalls++;
        }
    }
}
