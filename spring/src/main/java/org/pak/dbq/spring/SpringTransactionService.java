package org.pak.dbq.spring;

import org.pak.dbq.spi.TransactionService;
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
