package org.pak.dbq.internal.consumer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.pak.dbq.api.ConsumerConfig;
import org.pak.dbq.spi.MessageFactory;
import org.pak.dbq.spi.QueryService;
import org.pak.dbq.spi.TransactionService;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public class ConsumerStarter<T> {
    private final QueryService queryService;
    private final TransactionService transactionService;
    private final ConsumerConfig<T> consumerConfig;
    private final int concurrency;
    private final MessageFactory messageFactory;
    private ExecutorService fixedThreadPoolExecutor;
    private List<Consumer<T>> consumers = List.of();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    public ConsumerStarter(
            ConsumerConfig<T> consumerConfig,
            QueryService queryService,
            TransactionService transactionService,
            MessageFactory messageFactory
    ) {
        this.consumerConfig = consumerConfig;
        this.concurrency = consumerConfig.getProperties().getConcurrency();
        this.messageFactory = messageFactory;
        this.queryService = queryService;
        this.transactionService = transactionService;
    }

    public void start() {
        if (isRunning.compareAndSet(false, true)) {
            fixedThreadPoolExecutor = createExecutor();
            consumers = IntStream.range(0, concurrency).boxed()
                    .map(i -> {
                        var taskExecutor = new Consumer<>(
                                consumerConfig.getMessageHandler(),
                                consumerConfig.getQueueName(),
                                consumerConfig.getSubscriptionId(),
                                consumerConfig.getRetryablePolicy(),
                                consumerConfig.getNonRetryablePolicy(),
                                consumerConfig.getBlockingPolicy(),
                                queryService,
                                transactionService,
                                consumerConfig.getMessageContextPropagator(),
                                consumerConfig.getMessageConsumerTelemetry(),
                                messageFactory,
                                consumerConfig.getProperties());
                        fixedThreadPoolExecutor.submit(taskExecutor::poolLoop);
                        return taskExecutor;
                    }).toList();
        } else {
            log.warn("Consumer starter should be started only once");
        }
    }

    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            consumers.forEach(Consumer::stop);

            fixedThreadPoolExecutor.shutdown();
            try {
                if (fixedThreadPoolExecutor.awaitTermination(30, SECONDS)) {
                    log.info("Consumer stopped");
                } else {
                    log.warn("Consumer did not stop in time");
                    fixedThreadPoolExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.warn("Consumer did not stop in time", e);
                Thread.currentThread().interrupt();
            } finally {
                fixedThreadPoolExecutor = null;
                consumers = List.of();
            }
        } else {
            log.warn("Consumer starter should be stopped only once");
        }
    }

    private ExecutorService createExecutor() {
        return Executors.newFixedThreadPool(concurrency,
                r -> new ThreadFactoryBuilder()
                        .setNameFormat(consumerConfig.getQueueName() + "-processor-%d")
                        .setDaemon(true)
                        .setUncaughtExceptionHandler((t, e) -> {
                            log.error("Uncaught exception in thread {}", t.getName(), e);
                        })
                        .build()
                        .newThread(r));
    }
}
