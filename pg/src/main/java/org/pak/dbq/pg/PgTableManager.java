package org.pak.dbq.pg;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.spi.error.RetryablePersistenceException;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

@Slf4j
public class PgTableManager {
    public static final String TABLE_MANAGER = "tableManager";
    public static final String QUEUE_GROUP = "queue";

    private final PgQueryService pgQueryService;
    private final String cronCreatePartitions;
    private final String cronDropPartitions;
    private final Map<QueueName, Integer> queueNameStorageDays = new ConcurrentHashMap<>();
    private final Map<SubscriptionId, Integer> historyStorageDays = new ConcurrentHashMap<>();
    private Scheduler scheduler;

    public PgTableManager(PgQueryService pgQueryService, String cronCreatePartitions, String cronDropPartitions) {
        this.pgQueryService = pgQueryService;
        this.cronCreatePartitions = cronCreatePartitions;
        this.cronDropPartitions = cronDropPartitions;
    }

    public void registerQueue(QueueName queueName, int storageDays) {
        pgQueryService.createQueuePartition(queueName, Instant.now());
        pgQueryService.createQueuePartition(queueName, Instant.now().plus(1, ChronoUnit.DAYS));
        queueNameStorageDays.putIfAbsent(queueName, storageDays);
    }

    public void registerSubscription(QueueName queueName, SubscriptionId subscriptionId, int storageDays) {
        pgQueryService.createHistoryPartition(subscriptionId, Instant.now());
        pgQueryService.createHistoryPartition(subscriptionId, Instant.now().plus(1, ChronoUnit.DAYS));
        historyStorageDays.putIfAbsent(subscriptionId, storageDays);
    }

    public void startCronJobs() {
        try {
            var schedulerFactory = new StdSchedulerFactory();
            scheduler = schedulerFactory.getScheduler();

            scheduler.getContext().putIfAbsent(TABLE_MANAGER, this);

            addCronJob("createPartitions", CreatingPartitionsCronJob.class, cronCreatePartitions, scheduler);
            addCronJob("clearPartitions", CleaningPartitionsCronJob.class, cronDropPartitions, scheduler);

            scheduler.start();
        } catch (Exception e) {
            log.error("Unpredicted exception during starting cron jobs", e);
            if (RuntimeException.class.isAssignableFrom(e.getClass())) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    @SneakyThrows
    public void stopCronJobs() {
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.clear();
            scheduler.shutdown();
        }
    }

    @SneakyThrows
    public void createPartitions() {
        var date = Instant.now().plus(Duration.ofDays(1));

        queueNameStorageDays.keySet().forEach(queueName -> pgQueryService.createQueuePartition(queueName, date));
        historyStorageDays.keySet().forEach(subscriptionId -> pgQueryService.createHistoryPartition(subscriptionId, date));
    }

    public void cleanPartitions() {
        historyStorageDays.forEach((subscriptionId, storageDays) -> {
            var partitions = pgQueryService.getAllHistoryPartitions(subscriptionId);
            partitions.stream()
                    .filter(partition -> partition.isBefore(LocalDate.now().minusDays(storageDays)))
                    .forEach(partition -> {
                        try {
                            log.info("Dropping history subscription partition {} for {}", partition,
                                    subscriptionId.id());
                            pgQueryService.dropHistoryPartition(subscriptionId, partition);
                        } catch (PartitionHasReferencesException e) {
                            log.warn("Partition {} for history {} still has references, skipping", partition,
                                    subscriptionId.id());
                        }
                    });
        });

        queueNameStorageDays.forEach((queueName, storageDays) -> {
            var partitions = pgQueryService.getAllQueuePartitions(queueName);
            partitions.stream()
                    .filter(partition -> partition.isBefore(LocalDate.now().minusDays(storageDays)))
                    .forEach(partition -> {
                        try {
                            log.info("Dropping message partition {} for {}", partition, queueName.name());
                            pgQueryService.dropQueuePartition(queueName, partition);
                        } catch (PartitionHasReferencesException e) {
                            log.warn("Partition {} for queue {} still has references, skipping", partition,
                                    queueName.name());
                        }
                    });
        });
    }

    private void addCronJob(String jobKey, Class<? extends Job> clazz, String cron, Scheduler scheduler)
            throws SchedulerException {
        var key = new JobKey(jobKey, QUEUE_GROUP);
        var job = JobBuilder.newJob(clazz)
                .withIdentity(key)
                .build();
        var trigger = TriggerBuilder.newTrigger()
                .startNow()
                .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                .forJob(key)
                .build();
        scheduler.scheduleJob(job, trigger);
    }

    public static class CreatingPartitionsCronJob implements Job {
        @Override
        public void execute(JobExecutionContext context) {
            doJob(context, PgTableManager::createPartitions);
        }
    }

    public static class CleaningPartitionsCronJob implements Job {
        @Override
        public void execute(JobExecutionContext context) {
            doJob(context, PgTableManager::cleanPartitions);
        }
    }

    private static void doJob(JobExecutionContext context, Consumer<PgTableManager> job) {
        try {
            do {
                try {
                    job.accept(((PgTableManager) context.getScheduler().getContext().get(TABLE_MANAGER)));
                    break;
                } catch (SchedulerException e) {
                    log.error("Unpredicted exception during getting table manager", e);
                    throw new IllegalArgumentException(e);
                } catch (RetryablePersistenceException e) {
                    log.warn("Retryable persistence exception during job execution", e);
                    Thread.sleep(5000);
                } catch (Exception e) {
                    log.error("Unpredicted exception during job execution", e);
                    break;
                }
            } while (true);
        } catch (InterruptedException e) {
            log.error("Interrupted exception during job execution", e);
            Thread.currentThread().interrupt();
        }
    }
}
