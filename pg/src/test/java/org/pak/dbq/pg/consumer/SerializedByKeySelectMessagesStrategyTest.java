package org.pak.dbq.pg.consumer;

import org.junit.jupiter.api.Test;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.pg.SchemaName;
import org.pak.dbq.spi.PersistenceService;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SerializedByKeySelectMessagesStrategyTest {

    @Test
    void selectMessagesThrowsWhenKeyLockTableDoesNotExist() throws Exception {
        var persistenceService = mock(PersistenceService.class);
        when(persistenceService.query(any(String.class), any())).thenReturn(List.of(false));
        var strategy = new SerializedByKeySelectMessagesStrategy(
                new SchemaName("public"),
                new SubscriptionId("test-subscription"),
                "test_queue",
                10,
                persistenceService,
                mock(MessageContainerMapper.class));

        assertThatThrownBy(strategy::selectMessages)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("without serializedByKey support");
    }

    @Test
    void selectMessagesQueriesMessagesWhenKeyLockTableExists() throws Exception {
        var persistenceService = mock(PersistenceService.class);
        var mapper = mock(MessageContainerMapper.class);
        var expected = List.<MessageContainer<String>>of(TestMessageContainers.message());
        doReturn(List.of(true), expected).when(persistenceService).query(any(String.class), any());
        var strategy = new SerializedByKeySelectMessagesStrategy(
                new SchemaName("public"),
                new SubscriptionId("test-subscription"),
                "test_queue",
                10,
                persistenceService,
                mapper);

        var actual = strategy.selectMessages();

        assertThat(actual).isSameAs(expected);
        verify(persistenceService, times(2)).query(any(String.class), any());
    }

    @Test
    void hasKeyLockTableUsesCache() throws Exception {
        var persistenceService = mock(PersistenceService.class);
        when(persistenceService.query(any(String.class), any())).thenReturn(List.of(true));
        var strategy = new SerializedByKeySelectMessagesStrategy(
                new SchemaName("public"),
                new SubscriptionId("test-subscription"),
                "test_queue",
                10,
                persistenceService,
                mock(MessageContainerMapper.class));

        assertThat(strategy.hasKeyLockTable()).isTrue();
        assertThat(strategy.hasKeyLockTable()).isTrue();

        verify(persistenceService, times(1)).query(any(String.class), any());
    }

    @Test
    void hasKeyLockTableCachesFalseResult() throws Exception {
        var persistenceService = mock(PersistenceService.class);
        when(persistenceService.query(any(String.class), any())).thenReturn(List.of(false));
        var strategy = new SerializedByKeySelectMessagesStrategy(
                new SchemaName("public"),
                new SubscriptionId("test-subscription"),
                "test_queue",
                10,
                persistenceService,
                mock(MessageContainerMapper.class));

        assertThat(strategy.hasKeyLockTable()).isFalse();
        assertThat(strategy.hasKeyLockTable()).isFalse();

        verify(persistenceService, times(1)).query(any(String.class), any());
    }
}
