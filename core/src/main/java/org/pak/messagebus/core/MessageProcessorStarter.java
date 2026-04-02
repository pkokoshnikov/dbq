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
class MessageProcessorStarter<T> {
    private final QueryService queryService;
    private final TransactionService transactionService;
    private final SubscriberConfig<T> subscriberConfig;
    private final int concurrency;
    private final MessageFactory messageFactory;
    private ExecutorService fixedThreadPoolExecutor;
    private List<MessageProcessor<T>> messageProcessors = List.of();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    MessageProcessorStarter(
            SubscriberConfig<T> subscriberConfig,
            QueryService queryService,
            TransactionService transactionService,
            MessageFactory messageFactory
    ) {
        this.subscriberConfig = subscriberConfig;
        this.concurrency = subscriberConfig.getProperties().getConcurrency();
        this.messageFactory = messageFactory;
        this.queryService = queryService;
        this.transactionService = transactionService;
    }

    public void start() {
        if (isRunning.compareAndSet(false, true)) {
            fixedThreadPoolExecutor = createExecutor();
            messageProcessors = IntStream.range(0, concurrency).boxed()
                    .map(i -> {
                        var taskExecutor = new MessageProcessor<>(
                                subscriberConfig.getMessageListener(),
                                subscriberConfig.getMessageName(),
                                subscriberConfig.getSubscriptionName(),
                                subscriberConfig.getRetryablePolicy(),
                                subscriberConfig.getNonRetryablePolicy(),
                                subscriberConfig.getBlockingPolicy(),
                                queryService,
                                transactionService,
                                subscriberConfig.getTraceIdExtractor(),
                                messageFactory,
                                subscriberConfig.getProperties());
                        fixedThreadPoolExecutor.submit(taskExecutor::poolLoop);
                        return taskExecutor;
                    }).toList();
        } else {
            log.warn("Event processor starter should be started only once");
        }
    }

    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            messageProcessors.forEach(MessageProcessor::stop);

            fixedThreadPoolExecutor.shutdown();
            try {
                if (fixedThreadPoolExecutor.awaitTermination(30, SECONDS)) {
                    log.info("Event executor stopped");
                } else {
                    log.warn("Event executor did not stop in time");
                    fixedThreadPoolExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.warn("Event executor did not stop in time", e);
                Thread.currentThread().interrupt();
            } finally {
                fixedThreadPoolExecutor = null;
                messageProcessors = List.of();
            }
        } else {
            log.warn("Event processor starter should be stopped only once");
        }
    }

    private ExecutorService createExecutor() {
        return Executors.newFixedThreadPool(concurrency,
                r -> new ThreadFactoryBuilder()
                        .setNameFormat(subscriberConfig.getMessageName() + "-processor-%d")
                        .setDaemon(true)
                        .setUncaughtExceptionHandler((t, e) -> {
                            log.error("Uncaught exception in thread {}", t.getName(), e);
                        })
                        .build()
                        .newThread(r));
    }
}
