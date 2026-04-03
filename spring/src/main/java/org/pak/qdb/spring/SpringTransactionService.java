package org.pak.qdb.spring;

import org.pak.qdb.spi.TransactionService;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.function.Supplier;

public class SpringTransactionService implements TransactionService {
    private final TransactionTemplate transactionTemplate;

    public SpringTransactionService(TransactionTemplate transactionTemplate) {
        this.transactionTemplate = transactionTemplate;
    }

    @Override
    public <T> T inTransaction(Supplier<T> runnable) {
        return transactionTemplate.execute(status -> runnable.get());
    }
}
