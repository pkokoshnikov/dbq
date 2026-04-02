package org.pak.messagebus.core;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.pak.messagebus.core.service.QueryService;
import org.pak.messagebus.core.service.TransactionService;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
class QueueProcessorStarter<T> {
    private final QueryService queryService;
    private final TransactionService transactionService;
    private final ConsumerConfig<T> consumerConfig;
    private final int concurrency;
    private final MessageFactory messageFactory;
    private ExecutorService fixedThreadPoolExecutor;
    private List<QueueProcessor<T>> queueProcessors = List.of();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    QueueProcessorStarter(
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
            queueProcessors = IntStream.range(0, concurrency).boxed()
                    .map(i -> {
                        var taskExecutor = new QueueProcessor<>(
                                consumerConfig.getConsumer(),
                                consumerConfig.getQueueName(),
                                consumerConfig.getSubscriptionName(),
                                consumerConfig.getRetryablePolicy(),
                                consumerConfig.getNonRetryablePolicy(),
                                consumerConfig.getBlockingPolicy(),
                                queryService,
                                transactionService,
                                consumerConfig.getTraceIdExtractor(),
                                messageFactory,
                                consumerConfig.getProperties());
                        fixedThreadPoolExecutor.submit(taskExecutor::poolLoop);
                        return taskExecutor;
                    }).toList();
        } else {
            log.warn("Queue processor starter should be started only once");
        }
    }

    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            queueProcessors.forEach(QueueProcessor::stop);

            fixedThreadPoolExecutor.shutdown();
            try {
                if (fixedThreadPoolExecutor.awaitTermination(30, SECONDS)) {
                    log.info("Queue processor executor stopped");
                } else {
                    log.warn("Queue processor executor did not stop in time");
                    fixedThreadPoolExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.warn("Queue processor executor did not stop in time", e);
                Thread.currentThread().interrupt();
            } finally {
                fixedThreadPoolExecutor = null;
                queueProcessors = List.of();
            }
        } else {
            log.warn("Queue processor starter should be stopped only once");
        }
    }

    private ExecutorService createExecutor() {
        return Executors.newFixedThreadPool(concurrency,
                r -> new ThreadFactoryBuilder()
                        .setNameFormat(consumerConfig.getQueueName().name() + "-processor-%d")
                        .setDaemon(true)
                        .setUncaughtExceptionHandler((t, e) -> {
                            log.error("Uncaught exception in thread {}", t.getName(), e);
                        })
                        .build()
                        .newThread(r));
    }
}
