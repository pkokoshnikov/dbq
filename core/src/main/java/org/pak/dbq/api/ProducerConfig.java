package org.pak.dbq.api;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.FieldDefaults;
import org.pak.dbq.internal.support.NoOpMessageContextPropagator;
import org.pak.dbq.spi.MessageContextPropagator;

import java.util.Objects;

@FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
@Getter
public class ProducerConfig<T> {
    @NonNull
    QueueName queueName;
    @NonNull
    Class<T> clazz;
    MessageContextPropagator messageContextPropagator;

    @Builder
    public ProducerConfig(
            @NonNull QueueName queueName,
            @NonNull Class<T> clazz,
            MessageContextPropagator messageContextPropagator
    ) {
        this.queueName = Objects.requireNonNull(queueName, "queueName");
        this.clazz = Objects.requireNonNull(clazz, "clazz");
        this.messageContextPropagator = messageContextPropagator != null
                ? messageContextPropagator
                : new NoOpMessageContextPropagator();
    }
}
