package org.pak.messagebus.core;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CoreBasicsTest {
    @Test
    void messageNameRejectsInvalidFormat() {
        var exception = assertThrows(IllegalArgumentException.class, () -> new MessageName("Invalid_Name"));

        assertThat(exception.getMessage()).isEqualTo("Event name must be lowercase and -");
    }

    @Test
    void subscriptionNameRejectsInvalidFormat() {
        var exception = assertThrows(IllegalArgumentException.class, () -> new SubscriptionName("Invalid_Name"));

        assertThat(exception.getMessage()).isEqualTo("Subscription name must be lowercase and -");
    }

    @Test
    void schemaNameRejectsInvalidFormat() {
        var exception = assertThrows(IllegalArgumentException.class, () -> new SchemaName("invalid-schema"));

        assertThat(exception.getMessage()).isEqualTo("Schema name must be lowercase");
    }

    @Test
    void stdRetryablePolicyUsesExponentialBackoffAndCapsDelay() {
        var policy = new StdRetryablePolicy();

        assertThat(policy.apply(new RuntimeException("boom"), 0)).contains(Duration.ofSeconds(1));
        assertThat(policy.apply(new RuntimeException("boom"), 3)).contains(Duration.ofSeconds(8));
        assertThat(policy.apply(new RuntimeException("boom"), 20)).contains(Duration.ofMinutes(10));
        assertThat(policy.apply(new RuntimeException("boom"), 10001)).isEmpty();
    }

    @Test
    void cronConfigProvidesDefaultCronExpressions() {
        var config = CronConfig.builder().build();

        assertThat(config.getCreatingPartitionsCron()).isEqualTo("0 0 1 * * ?");
        assertThat(config.getCleaningPartitionsCron()).isEqualTo("0 0 2 * * ?");
    }
}
