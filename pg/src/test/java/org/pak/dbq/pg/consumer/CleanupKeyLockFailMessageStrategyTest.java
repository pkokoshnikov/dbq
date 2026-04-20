package org.pak.dbq.pg.consumer;

import org.junit.jupiter.api.Test;
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

class CleanupKeyLockFailMessageStrategyTest {

    @Test
    void failMessageCallsDelegateThenCleansUpKeyLock() throws Exception {
        var subscriptionId = new SubscriptionId("test-subscription");
        var delegate = mock(FailMessageStrategy.class);
        var persistenceService = mock(PersistenceService.class);
        var strategy = new CleanupKeyLockFailMessageStrategy(
                subscriptionId,
                delegate,
                new SchemaName("public"),
                persistenceService);
        var message = new MessageContainer<>(
                BigInteger.ONE,
                BigInteger.ONE,
                "test-key",
                0,
                Instant.now(),
                Instant.now(),
                Instant.now(),
                Instant.now(),
                "payload",
                Map.of(),
                null,
                null);
        var exception = new RuntimeException("boom");

        strategy.failMessage(message, exception);

        var inOrder = inOrder(delegate, persistenceService);
        inOrder.verify(delegate).failMessage(message, exception);
        inOrder.verify(persistenceService).update(
                any(String.class),
                eq("test-key"),
                eq("test-key"));
        verifyNoMoreInteractions(delegate, persistenceService);
    }

    @Test
    void failMessageDoesNotCleanupKeyLockWhenDelegateFails() throws Exception {
        var delegate = mock(FailMessageStrategy.class);
        var persistenceService = mock(PersistenceService.class);
        var strategy = new CleanupKeyLockFailMessageStrategy(
                new SubscriptionId("test-subscription"),
                delegate,
                new SchemaName("public"),
                persistenceService);
        var message = TestMessageContainers.message();
        var exception = new RuntimeException("boom");
        doThrow(exception).when(delegate).failMessage(message, exception);

        assertThatThrownBy(() -> strategy.failMessage(message, exception))
                .isSameAs(exception);

        verify(delegate).failMessage(message, exception);
        verifyNoMoreInteractions(delegate, persistenceService);
    }
}
