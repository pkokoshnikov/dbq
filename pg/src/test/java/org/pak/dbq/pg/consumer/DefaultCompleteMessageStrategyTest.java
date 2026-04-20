package org.pak.dbq.pg.consumer;

import org.junit.jupiter.api.Test;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.pg.SchemaName;
import org.pak.dbq.spi.PersistenceService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DefaultCompleteMessageStrategyTest {

    @Test
    void completeMessageDeletesRecordById() throws Exception {
        var persistenceService = mock(PersistenceService.class);
        when(persistenceService.update(any(String.class), any())).thenReturn(1);
        var strategy = new DefaultCompleteMessageStrategy(
                new SchemaName("public"),
                new SubscriptionId("test-subscription"),
                persistenceService);

        strategy.completeMessage(TestMessageContainers.message());

        verify(persistenceService, times(1)).update(any(String.class), eq(TestMessageContainers.message().getId()));
    }
}
