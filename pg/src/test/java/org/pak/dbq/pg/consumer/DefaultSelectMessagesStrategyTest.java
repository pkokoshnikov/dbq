package org.pak.dbq.pg.consumer;

import org.junit.jupiter.api.Test;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.internal.persistence.MessageContainer;
import org.pak.dbq.pg.SchemaName;
import org.pak.dbq.spi.PersistenceService;

import java.sql.ResultSet;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class DefaultSelectMessagesStrategyTest {

    @Test
    void selectMessagesDelegatesToPersistenceServiceQuery() throws Exception {
        var persistenceService = mock(PersistenceService.class);
        var mapper = mock(MessageContainerMapper.class);
        var expected = List.<MessageContainer<String>>of(TestMessageContainers.message());
        doReturn(expected).when(persistenceService).query(any(String.class), any());
        var strategy = new DefaultSelectMessagesStrategy(
                new SchemaName("public"),
                new SubscriptionId("test-subscription"),
                "test_queue",
                10,
                persistenceService,
                mapper);

        var actual = strategy.selectMessages();

        assertThat(actual).isSameAs(expected);
        verify(persistenceService).query(any(String.class), any());
    }
}
