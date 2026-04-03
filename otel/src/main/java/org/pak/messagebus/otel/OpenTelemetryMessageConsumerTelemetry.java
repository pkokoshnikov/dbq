package org.pak.messagebus.otel;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import org.pak.messagebus.core.Message;
import org.pak.messagebus.core.MessageConsumerTelemetry;
import org.pak.messagebus.core.QueueName;
import org.pak.messagebus.core.SubscriptionId;
import org.slf4j.MDC;

public final class OpenTelemetryMessageConsumerTelemetry implements MessageConsumerTelemetry {
    private static final String INSTRUMENTATION_NAME = "org.pak.messagebus.otel";
    private final Tracer tracer;

    public OpenTelemetryMessageConsumerTelemetry() {
        this(GlobalOpenTelemetry.get());
    }

    public OpenTelemetryMessageConsumerTelemetry(OpenTelemetry openTelemetry) {
        this(openTelemetry.getTracer(INSTRUMENTATION_NAME));
    }

    public OpenTelemetryMessageConsumerTelemetry(Tracer tracer) {
        this.tracer = tracer;
    }

    @Override
    public <T> Scope start(Message<T> message, QueueName queueName, SubscriptionId subscriptionId) {
        var span = tracer.spanBuilder(queueName.name() + " process")
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute("messaging.system", "message-bus")
                .setAttribute("messaging.operation", "process")
                .setAttribute("messaging.destination.name", queueName.name())
                .setAttribute("messaging.destination.subscription.name", subscriptionId.id())
                .setAttribute("messagebus.message.key", message.key())
                .startSpan();
        var scope = span.makeCurrent();
        var traceId = MDC.putCloseable("traceId", span.getSpanContext().getTraceId());
        var spanId = MDC.putCloseable("spanId", span.getSpanContext().getSpanId());

        return new Scope() {
            @Override
            public void recordError(Exception exception) {
                span.recordException(exception);
                span.setStatus(StatusCode.ERROR);
            }

            @Override
            public void close() {
                closeQuietly(spanId);
                closeQuietly(traceId);
                scope.close();
                span.end();
            }
        };
    }

    private void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
            // no-op
        }
    }
}
