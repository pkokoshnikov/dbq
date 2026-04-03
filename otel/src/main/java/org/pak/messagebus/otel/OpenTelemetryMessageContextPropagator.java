package org.pak.messagebus.otel;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.pak.messagebus.core.MessageContextPropagator;
import org.slf4j.MDC;

import java.util.LinkedHashMap;
import java.util.Map;

public final class OpenTelemetryMessageContextPropagator implements MessageContextPropagator {
    private static final TextMapSetter<Map<String, String>> SETTER = Map::put;
    private static final TextMapGetter<Map<String, String>> GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier.get(key);
        }
    };

    private final TextMapPropagator propagator;

    public OpenTelemetryMessageContextPropagator() {
        this(GlobalOpenTelemetry.get());
    }

    public OpenTelemetryMessageContextPropagator(OpenTelemetry openTelemetry) {
        this(openTelemetry.getPropagators().getTextMapPropagator());
    }

    public OpenTelemetryMessageContextPropagator(TextMapPropagator propagator) {
        this.propagator = propagator;
    }

    @Override
    public Map<String, String> injectCurrentContext(Map<String, String> headers) {
        var carrier = new LinkedHashMap<>(headers);
        propagator.inject(Context.current(), carrier, SETTER);
        return Map.copyOf(carrier);
    }

    @Override
    public Scope extractToCurrentContext(Map<String, String> headers) {
        var extractedContext = propagator.extract(Context.root(), headers, GETTER);
        var scope = extractedContext.makeCurrent();
        var spanContext = Span.fromContext(extractedContext).getSpanContext();
        var traceId = putIfValid("traceId", spanContext, spanContext.getTraceId());
        var spanId = putIfValid("spanId", spanContext, spanContext.getSpanId());

        return () -> {
            closeQuietly(spanId);
            closeQuietly(traceId);
            scope.close();
        };
    }

    private MDC.MDCCloseable putIfValid(String key, SpanContext spanContext, String value) {
        if (!spanContext.isValid()) {
            return null;
        }
        return MDC.putCloseable(key, value);
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
