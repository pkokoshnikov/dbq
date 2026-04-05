package org.pak.dbq.spi;

import java.util.function.Supplier;

public interface TransactionService {
    <T> T inTransaction(Supplier<T> runnable);
}
