package org.pak.dbq.internal;

import org.junit.jupiter.api.Test;
import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.api.QueueConfig;
import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.api.policy.SimpleRetryablePolicy;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CoreBasicsTest {
    @Test
    void messageNameRejectsInvalidFormat() {
        var exception = assertThrows(IllegalArgumentException.class, () -> new QueueName("Invalid_Name"));

        assertThat(exception.getMessage()).isEqualTo("Queue name must be lowercase and -");
    }

    @Test
    void subscriptionIdRejectsInvalidFormat() {
        var exception = assertThrows(IllegalArgumentException.class, () -> new SubscriptionId("Invalid_Name"));

        assertThat(exception.getMessage()).isEqualTo("Subscription id must be lowercase and -");
    }



    @Test
    void simpleRetryablePolicyUsesExponentialBackoffAndCapsDelay() {
        var policy = new SimpleRetryablePolicy();

        assertThat(policy.apply(new RuntimeException("boom"), 0)).contains(Duration.ofSeconds(1));
        assertThat(policy.apply(new RuntimeException("boom"), 3)).contains(Duration.ofSeconds(8));
        assertThat(policy.apply(new RuntimeException("boom"), 12)).contains(Duration.ofMinutes(60));
        assertThat(policy.apply(new RuntimeException("boom"), 20)).contains(Duration.ofMinutes(60));
        assertThat(policy.apply(new RuntimeException("boom"), Integer.MAX_VALUE)).isEmpty();
    }

    @Test
    void consumerPropertiesRejectNonPositiveConcurrency() {
        var exception = assertThrows(IllegalArgumentException.class, () -> ConsumerConfig.Properties.builder()
                .concurrency(0)
                .build());

        assertThat(exception.getMessage()).isEqualTo("concurrency must be > 0");
    }

    @Test
    void consumerPropertiesRejectNonPositiveMaxPollRecords() {
        var exception = assertThrows(IllegalArgumentException.class, () -> ConsumerConfig.Properties.builder()
                .maxPollRecords(0)
                .build());

        assertThat(exception.getMessage()).isEqualTo("maxPollRecords must be > 0");
    }

    @Test
    void consumerPropertiesRejectNegativePauses() {
        var exception = assertThrows(IllegalArgumentException.class, () -> ConsumerConfig.Properties.builder()
                .persistenceExceptionPause(Duration.ofSeconds(-1))
                .build());

        assertThat(exception.getMessage()).isEqualTo("persistenceExceptionPause must be >= 0");
    }

    @Test
    void queuePropertiesRejectNonPositiveRetentionDays() {
        var exception = assertThrows(IllegalArgumentException.class, () -> QueueConfig.Properties.builder()
                .retentionDays(-1)
                .build());

        assertThat(exception.getMessage()).isEqualTo("retentionDays must be > 0");
    }
}
