package org.pak.messagebus.core;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.FieldDefaults;

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
    TraceIdExtractor<T> traceIdExtractor = new NullTraceIdExtractor<>();

    @Builder
    @Getter
    @FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
    public static class Properties {
        @Builder.Default
        int storageDays = 30;
    }
}
