package org.pak.dbq.pg.consumer;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.pg.SchemaName;
import org.pak.dbq.spi.PersistenceService;

import java.math.BigInteger;
import java.time.Instant;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CleanupKeyLockCompleteMessageStrategyTest {

    @Test
    void completeMessageCallsDelegateThenCleansUpKeyLock() throws Exception {
        var subscriptionId = new SubscriptionId("test-subscription");
        var delegate = mock(CompleteMessageStrategy.class);
        var persistenceService = mock(PersistenceService.class);
        var strategy = new CleanupKeyLockCompleteMessageStrategy(
                subscriptionId,
                delegate,
                new SchemaName("public"),
                persistenceService);
        var message = TestMessageContainers.message();

        strategy.completeMessage(message);

        InOrder inOrder = inOrder(delegate, persistenceService);
        inOrder.verify(delegate).completeMessage(message);
        inOrder.verify(persistenceService).update(any(String.class), eq("test-key"), eq("test-key"));
        verifyNoMoreInteractions(delegate, persistenceService);
    }

    @Test
    void completeMessageDoesNotCleanupKeyLockWhenDelegateFails() throws Exception {
        var delegate = mock(CompleteMessageStrategy.class);
        var persistenceService = mock(PersistenceService.class);
        var strategy = new CleanupKeyLockCompleteMessageStrategy(
                new SubscriptionId("test-subscription"),
                delegate,
                new SchemaName("public"),
                persistenceService);
        var message = TestMessageContainers.message();
        var exception = new RuntimeException("boom");
        doThrow(exception).when(delegate).completeMessage(message);

        assertThatThrownBy(() -> strategy.completeMessage(message))
                .isSameAs(exception);

        verify(delegate).completeMessage(message);
        verifyNoMoreInteractions(delegate, persistenceService);
    }
}
