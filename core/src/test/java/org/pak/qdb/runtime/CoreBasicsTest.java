package org.pak.qdb.runtime;

import org.junit.jupiter.api.Test;
import org.pak.qdb.api.QueueName;
import org.pak.qdb.api.SchemaName;
import org.pak.qdb.api.SubscriptionId;
import org.pak.qdb.policy.SimpleRetryablePolicy;

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
    void schemaNameRejectsInvalidFormat() {
        var exception = assertThrows(IllegalArgumentException.class, () -> new SchemaName("invalid-schema"));

        assertThat(exception.getMessage()).isEqualTo("Schema name must be lowercase");
    }

    @Test
    void simpleRetryablePolicyUsesExponentialBackoffAndCapsDelay() {
        var policy = new SimpleRetryablePolicy();

        assertThat(policy.apply(new RuntimeException("boom"), 0)).contains(Duration.ofSeconds(1));
        assertThat(policy.apply(new RuntimeException("boom"), 3)).contains(Duration.ofSeconds(8));
        assertThat(policy.apply(new RuntimeException("boom"), 20)).contains(Duration.ofMinutes(10));
        assertThat(policy.apply(new RuntimeException("boom"), 10001)).isEmpty();
    }
}
