package org.pak.dbq.pg.consumer;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.pg.QueuePartitionService;
import org.pak.dbq.pg.SchemaName;
import org.pak.dbq.spi.PersistenceService;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class HistoryCompleteMessageStrategyTest {

    @Test
    void completeMessageEnsuresPartitionThenMovesMessageToHistory() throws Exception {
        var persistenceService = mock(PersistenceService.class);
        var partitionService = mock(QueuePartitionService.class);
        when(persistenceService.update(any(String.class), any())).thenReturn(1);
        var subscriptionId = new SubscriptionId("test-subscription");
        var strategy = new HistoryCompleteMessageStrategy(
                new SchemaName("public"),
                subscriptionId,
                persistenceService,
                partitionService);
        var message = TestMessageContainers.message();

        strategy.completeMessage(message);

        InOrder inOrder = inOrder(partitionService, persistenceService);
        inOrder.verify(partitionService).ensureHistoryPartitionExists(message.getOriginatedTime(), subscriptionId);
        inOrder.verify(persistenceService).update(any(String.class), eq(message.getId()));
    }

    @Test
    void completeMessageDoesNotUpdateWhenEnsuringPartitionFails() throws Exception {
        var persistenceService = mock(PersistenceService.class);
        var partitionService = mock(QueuePartitionService.class);
        var subscriptionId = new SubscriptionId("test-subscription");
        var strategy = new HistoryCompleteMessageStrategy(
                new SchemaName("public"),
                subscriptionId,
                persistenceService,
                partitionService);
        var message = TestMessageContainers.message();
        var exception = new RuntimeException("partition failure");
        doThrow(exception).when(partitionService).ensureHistoryPartitionExists(message.getOriginatedTime(), subscriptionId);

        assertThatThrownBy(() -> strategy.completeMessage(message))
                .isSameAs(exception);

        verifyNoInteractions(persistenceService);
    }
}
