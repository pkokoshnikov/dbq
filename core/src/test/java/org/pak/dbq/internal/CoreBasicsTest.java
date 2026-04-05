package org.pak.dbq.internal;

import org.junit.jupiter.api.Test;
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
}
