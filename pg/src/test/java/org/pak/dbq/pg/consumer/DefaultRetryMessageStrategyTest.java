package org.pak.dbq.pg.consumer;

import org.junit.jupiter.api.Test;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.pg.SchemaName;
import org.pak.dbq.spi.PersistenceService;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DefaultRetryMessageStrategyTest {

    @Test
    void retryMessageUpdatesRetryFields() throws Exception {
        var persistenceService = mock(PersistenceService.class);
        when(persistenceService.update(any(String.class), any())).thenReturn(1);
        var strategy = new DefaultRetryMessageStrategy(
                new SchemaName("public"),
                new SubscriptionId("test-subscription"),
                persistenceService);
        var exception = new RuntimeException("boom");

        strategy.retryMessage(TestMessageContainers.message(), Duration.ofSeconds(42), exception);

        verify(persistenceService, times(1)).update(
                any(String.class),
                eq(42L),
                eq("boom"),
                any(String.class),
                eq(TestMessageContainers.message().getId()));
    }

    @Test
    void retryMessagePropagatesPersistenceException() throws Exception {
        var persistenceService = mock(PersistenceService.class);
        var strategy = new DefaultRetryMessageStrategy(
                new SchemaName("public"),
                new SubscriptionId("test-subscription"),
                persistenceService);
        var exception = new RuntimeException("boom");
        doThrow(exception).when(persistenceService).update(any(String.class), any(), any(), any(), any());

        assertThatThrownBy(() -> strategy.retryMessage(TestMessageContainers.message(), Duration.ofSeconds(1), exception))
                .isSameAs(exception);
    }
}
