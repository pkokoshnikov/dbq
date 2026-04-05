package org.pak.dbq.spi;

import java.util.Map;

public interface MessageContextPropagator {
    Map<String, String> injectCurrentContext(Map<String, String> headers);

    Scope extractToCurrentContext(Map<String, String> headers);

    interface Scope extends AutoCloseable {
        @Override
        void close();
    }
}
