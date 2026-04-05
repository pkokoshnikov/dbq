package org.pak.dbq.api;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.FieldDefaults;

import java.util.Objects;

@FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
@Getter
public class QueueConfig {
    @NonNull
    QueueName queueName;
    Properties properties;

    @Builder
    public QueueConfig(@NonNull QueueName queueName, Properties properties) {
        this.queueName = Objects.requireNonNull(queueName, "queueName");
        this.properties = properties != null ? properties : Properties.builder().build();
    }

    @Builder
    @Getter
    @FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
    public static class Properties {
        int retentionDays;

        @Builder
        public Properties(int retentionDays) {
            this.retentionDays = retentionDays == 0 ? 30 : retentionDays;
            validate();
        }

        private void validate() {
            if (retentionDays <= 0) {
                throw new IllegalArgumentException("retentionDays must be > 0");
            }
        }
    }
}
