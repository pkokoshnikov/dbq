package org.pak.qdb.otel;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.Test;
import org.pak.qdb.api.QueueName;
import org.pak.qdb.api.SubscriptionId;
import org.pak.qdb.model.SimpleMessage;
import org.slf4j.MDC;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class OpenTelemetryMessageConsumerTelemetryTest {
    @Test
    void startCreatesConsumerSpanWithCurrentContextAsParent() {
        var spanExporter = InMemorySpanExporter.create();
        var tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();
        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .build();
        var tracer = openTelemetry.getTracer("test");
        var telemetry = new OpenTelemetryMessageConsumerTelemetry(openTelemetry);

        var parent = tracer.spanBuilder("parent").startSpan();
        try (var ignoredParentScope = parent.makeCurrent()) {
            try (var ignoredTelemetry = telemetry.start(
                    new SimpleMessage<>("message-key", Instant.parse("2026-04-03T10:15:30Z"), "payload"),
                    new QueueName("orders"),
                    new SubscriptionId("billing")
            )) {
                var currentSpan = Span.current().getSpanContext();

                assertThat(currentSpan.isValid()).isTrue();
                assertThat(currentSpan.getTraceId()).isEqualTo(parent.getSpanContext().getTraceId());
                assertThat(currentSpan.getSpanId()).isNotEqualTo(parent.getSpanContext().getSpanId());
                assertThat(MDC.get("traceId")).isEqualTo(currentSpan.getTraceId());
                assertThat(MDC.get("spanId")).isEqualTo(currentSpan.getSpanId());
            }
        } finally {
            parent.end();
        }

        var exportedSpans = spanExporter.getFinishedSpanItems();
        var consumerSpan = exportedSpans.stream()
                .filter(spanData -> spanData.getName().equals("orders process"))
                .findFirst()
                .orElseThrow();

        assertThat(consumerSpan.getParentSpanContext().getSpanId()).isEqualTo(parent.getSpanContext().getSpanId());
        assertThat(consumerSpan.getKind()).isEqualTo(io.opentelemetry.api.trace.SpanKind.CONSUMER);
        assertThat(consumerSpan.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey(
                "messaging.destination.name"))).isEqualTo("orders");
        assertThat(consumerSpan.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey(
                "messaging.destination.subscription.name"))).isEqualTo("billing");
        assertThat(consumerSpan.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey(
                "dbq.message.key"))).isEqualTo("message-key");
        assertThat(MDC.get("traceId")).isNull();
        assertThat(MDC.get("spanId")).isNull();
        tracerProvider.close();
    }

    @Test
    void recordErrorMarksSpanAsFailed() {
        var spanExporter = InMemorySpanExporter.create();
        var tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();
        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .build();
        var telemetry = new OpenTelemetryMessageConsumerTelemetry(openTelemetry);
        var exception = new IllegalStateException("boom");

        try (var ignoredTelemetry = telemetry.start(
                new SimpleMessage<>("message-key", Instant.parse("2026-04-03T10:15:30Z"), "payload"),
                new QueueName("orders"),
                new SubscriptionId("billing")
        )) {
            ignoredTelemetry.recordError(exception);
        }

        var consumerSpan = spanExporter.getFinishedSpanItems().getFirst();
        assertThat(consumerSpan.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
        assertThat(consumerSpan.getEvents())
                .anySatisfy(event -> assertThat(event.getName()).isEqualTo("exception"));
        tracerProvider.close();
    }
}
