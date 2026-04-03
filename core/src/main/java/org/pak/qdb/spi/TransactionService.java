package org.pak.qdb.spi;

import java.util.function.Supplier;

public interface TransactionService {
    <T> T inTransaction(Supplier<T> runnable);
}
