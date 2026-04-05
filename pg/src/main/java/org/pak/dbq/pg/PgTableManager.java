package org.pak.dbq.pg;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.spi.error.RetryablePersistenceException;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
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
    private final Clock clock;
    private final Map<QueueName, Integer> queueRetentionDays = new ConcurrentHashMap<>();
    private final Map<SubscriptionId, Integer> historyRetentionDays = new ConcurrentHashMap<>();
    private Scheduler scheduler;

    public PgTableManager(PgQueryService pgQueryService, String cronCreatePartitions, String cronDropPartitions) {
        this(pgQueryService, cronCreatePartitions, cronDropPartitions, Clock.systemUTC());
    }

    public PgTableManager(
            PgQueryService pgQueryService,
            String cronCreatePartitions,
            String cronDropPartitions,
            Clock clock
    ) {
        this.pgQueryService = pgQueryService;
        this.cronCreatePartitions = cronCreatePartitions;
        this.cronDropPartitions = cronDropPartitions;
        this.clock = clock.withZone(ZoneOffset.UTC);
    }

    public void registerQueue(QueueName queueName, int retentionDays) {
        var now = Instant.now(clock);
        pgQueryService.createQueuePartition(queueName, now);
        pgQueryService.createQueuePartition(queueName, now.plus(1, ChronoUnit.DAYS));
        queueRetentionDays.putIfAbsent(queueName, retentionDays);
    }

    public void registerSubscription(
            QueueName queueName,
            SubscriptionId subscriptionId,
            int retentionDays,
            boolean historyEnabled
    ) {
        if (!historyEnabled) {
            return;
        }

        var now = Instant.now(clock);
        pgQueryService.createHistoryPartition(subscriptionId, now);
        pgQueryService.createHistoryPartition(subscriptionId, now.plus(1, ChronoUnit.DAYS));
        historyRetentionDays.putIfAbsent(subscriptionId, retentionDays);
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
        var date = Instant.now(clock).plus(Duration.ofDays(1));

        queueRetentionDays.keySet().forEach(queueName -> pgQueryService.createQueuePartition(queueName, date));
        historyRetentionDays.keySet().forEach(subscriptionId -> pgQueryService.createHistoryPartition(subscriptionId, date));
    }

    public void cleanPartitions() {
        var todayUtc = LocalDate.now(clock);
        historyRetentionDays.forEach((subscriptionId, retentionDays) -> {
            var partitions = pgQueryService.getAllHistoryPartitions(subscriptionId);
            partitions.stream()
                    .filter(partition -> partition.isBefore(todayUtc.minusDays(retentionDays)))
                    .forEach(partition -> {
                        log.info("Dropping history subscription partition {} for {}", partition,
                                subscriptionId.id());
                        var result = pgQueryService.dropHistoryPartition(subscriptionId, partition);
                        if (result == PgQueryService.DropPartitionResult.HAS_REFERENCES) {
                            log.warn("Partition {} for history {} still has references, skipping", partition,
                                    subscriptionId.id());
                        }
                    });
        });

        queueRetentionDays.forEach((queueName, retentionDays) -> {
            var partitions = pgQueryService.getAllQueuePartitions(queueName);
            partitions.stream()
                    .filter(partition -> partition.isBefore(todayUtc.minusDays(retentionDays)))
                    .forEach(partition -> {
                        log.info("Dropping message partition {} for {}", partition, queueName.name());
                        var result = pgQueryService.dropQueuePartition(queueName, partition);
                        if (result == PgQueryService.DropPartitionResult.HAS_REFERENCES) {
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
