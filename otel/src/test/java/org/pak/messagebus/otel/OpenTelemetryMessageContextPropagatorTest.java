package org.pak.messagebus.otel;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OpenTelemetryMessageContextPropagatorTest {
    @Test
    void injectCurrentContextWritesW3cTraceHeaders() {
        var propagator = new OpenTelemetryMessageContextPropagator(W3CTraceContextPropagator.getInstance());
        var spanContext = SpanContext.create(
                "0123456789abcdef0123456789abcdef",
                "0123456789abcdef",
                TraceFlags.getSampled(),
                TraceState.getDefault()
        );

        try (var ignored = Context.root().with(Span.wrap(spanContext)).makeCurrent()) {
            var headers = propagator.injectCurrentContext(Map.of("tenant", "alpha"));

            assertThat(headers)
                    .containsEntry("tenant", "alpha")
                    .containsEntry("traceparent", "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01");
        }
    }

    @Test
    void extractToCurrentContextRestoresSpanContextAndMdc() {
        var propagator = new OpenTelemetryMessageContextPropagator(W3CTraceContextPropagator.getInstance());
        var headers = Map.of(
                "traceparent",
                "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
        );

        try (var ignored = propagator.extractToCurrentContext(headers)) {
            var spanContext = Span.fromContext(Context.current()).getSpanContext();

            assertThat(spanContext.isValid()).isTrue();
            assertThat(spanContext.getTraceId()).isEqualTo("0123456789abcdef0123456789abcdef");
            assertThat(spanContext.getSpanId()).isEqualTo("0123456789abcdef");
            assertThat(MDC.get("traceId")).isEqualTo("0123456789abcdef0123456789abcdef");
            assertThat(MDC.get("spanId")).isEqualTo("0123456789abcdef");
        }

        assertThat(MDC.get("traceId")).isNull();
        assertThat(MDC.get("spanId")).isNull();
    }
}
