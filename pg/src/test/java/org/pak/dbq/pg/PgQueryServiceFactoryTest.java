package org.pak.dbq.pg;

import org.junit.jupiter.api.Test;
import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.pg.consumer.CleanupKeyLockCompleteMessageStrategy;
import org.pak.dbq.pg.consumer.CleanupKeyLockFailMessageStrategy;
import org.pak.dbq.pg.consumer.DefaultCompleteMessageStrategy;
import org.pak.dbq.pg.consumer.DefaultFailMessageStrategy;
import org.pak.dbq.pg.consumer.DefaultRetryMessageStrategy;
import org.pak.dbq.pg.consumer.DefaultSelectMessagesStrategy;
import org.pak.dbq.pg.consumer.HistoryCompleteMessageStrategy;
import org.pak.dbq.pg.consumer.HistoryFailMessageStrategy;
import org.pak.dbq.pg.consumer.PgConsumerQueryService;
import org.pak.dbq.pg.consumer.SerializedByKeySelectMessagesStrategy;
import org.pak.dbq.pg.jsonb.JsonbConverter;
import org.pak.dbq.spi.PersistenceService;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class PgQueryServiceFactoryTest {

    @Test
    void createConsumerQueryServiceUsesDefaultStrategies() {
        var consumerQueryService = createFactory(mock(PersistenceService.class))
                .createConsumerQueryService(consumerConfig(false, false));

        assertThat(readField(consumerQueryService, "selectMessagesStrategy"))
                .isInstanceOf(DefaultSelectMessagesStrategy.class);
        assertThat(readField(consumerQueryService, "retryMessageStrategy"))
                .isInstanceOf(DefaultRetryMessageStrategy.class);
        assertThat(readField(consumerQueryService, "failMessageStrategy"))
                .isInstanceOf(DefaultFailMessageStrategy.class);
        assertThat(readField(consumerQueryService, "completeMessageStrategy"))
                .isInstanceOf(DefaultCompleteMessageStrategy.class);
    }

    @Test
    void createConsumerQueryServiceUsesHistoryStrategiesWhenHistoryIsEnabled() {
        var consumerQueryService = createFactory(mock(PersistenceService.class))
                .createConsumerQueryService(consumerConfig(true, false));

        assertThat(readField(consumerQueryService, "selectMessagesStrategy"))
                .isInstanceOf(DefaultSelectMessagesStrategy.class);
        assertThat(readField(consumerQueryService, "failMessageStrategy"))
                .isInstanceOf(HistoryFailMessageStrategy.class);
        assertThat(readField(consumerQueryService, "completeMessageStrategy"))
                .isInstanceOf(HistoryCompleteMessageStrategy.class);
    }

    @Test
    void createConsumerQueryServiceUsesKeyLockWrappersOnlyWhenSerializedByKeyIsEnabled() {
        var consumerQueryService = createFactory(mock(PersistenceService.class))
                .createConsumerQueryService(consumerConfig(false, true));

        assertThat(readField(consumerQueryService, "selectMessagesStrategy"))
                .isInstanceOf(SerializedByKeySelectMessagesStrategy.class);
        assertThat(readField(consumerQueryService, "failMessageStrategy"))
                .isInstanceOf(CleanupKeyLockFailMessageStrategy.class);
        assertThat(readField(consumerQueryService, "completeMessageStrategy"))
                .isInstanceOf(CleanupKeyLockCompleteMessageStrategy.class);
        assertThat(readField(readField(consumerQueryService, "failMessageStrategy"), "delegate"))
                .isInstanceOf(DefaultFailMessageStrategy.class);
        assertThat(readField(readField(consumerQueryService, "completeMessageStrategy"), "delegate"))
                .isInstanceOf(DefaultCompleteMessageStrategy.class);
    }

    @Test
    void createConsumerQueryServiceWrapsHistoryStrategiesWithKeyLockCleanupWhenNeeded() {
        var consumerQueryService = createFactory(mock(PersistenceService.class))
                .createConsumerQueryService(consumerConfig(true, true));

        assertThat(readField(consumerQueryService, "selectMessagesStrategy"))
                .isInstanceOf(SerializedByKeySelectMessagesStrategy.class);
        assertThat(readField(consumerQueryService, "failMessageStrategy"))
                .isInstanceOf(CleanupKeyLockFailMessageStrategy.class);
        assertThat(readField(consumerQueryService, "completeMessageStrategy"))
                .isInstanceOf(CleanupKeyLockCompleteMessageStrategy.class);
        assertThat(readField(readField(consumerQueryService, "failMessageStrategy"), "delegate"))
                .isInstanceOf(HistoryFailMessageStrategy.class);
        assertThat(readField(readField(consumerQueryService, "completeMessageStrategy"), "delegate"))
                .isInstanceOf(HistoryCompleteMessageStrategy.class);
    }

    private static PgQueryServiceFactory createFactory(PersistenceService persistenceService) {
        return new PgQueryServiceFactory(
                new QueueTableService(persistenceService, new SchemaName("public")),
                persistenceService,
                new SchemaName("public"),
                new JsonbConverter());
    }

    private static ConsumerConfig<String> consumerConfig(boolean historyEnabled, boolean serializedByKey) {
        return ConsumerConfig.<String>builder()
                .queueName(new QueueName("test-queue"))
                .subscriptionId(new SubscriptionId("test-subscription"))
                .messageHandler(message -> {
                })
                .properties(ConsumerConfig.Properties.builder()
                        .maxPollRecords(10)
                        .historyEnabled(historyEnabled)
                        .serializedByKey(serializedByKey)
                        .build())
                .build();
    }

    private static Object readField(Object target, String fieldName) {
        try {
            Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(target);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Failed to read field %s from %s".formatted(fieldName, target.getClass()), e);
        }
    }
}
