package org.pak.dbq.api;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.FieldDefaults;
import org.pak.dbq.spi.MessageContextPropagator;
import org.pak.dbq.internal.support.NoOpMessageContextPropagator;

@Builder
@FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
@Getter
public class ProducerConfig<T> {
    @NonNull
    QueueName queueName;
    @NonNull
    Class<T> clazz;
    Properties properties;
    @Builder.Default
    MessageContextPropagator messageContextPropagator = new NoOpMessageContextPropagator();

    @Builder
    @Getter
    @FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
    public static class Properties {
        @Builder.Default
        int retentionDays = 30;
    }
}
