package org.pak.dbq.pg;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.pak.dbq.api.QueueName;
import org.pak.dbq.api.SubscriptionId;
import org.pak.dbq.error.DbqException;
import org.pak.dbq.spi.TableManager;
import org.pak.dbq.error.RetryablePersistenceException;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class PgTableManager implements TableManager {
    public static final String TABLE_MANAGER = "tableManager";
    public static final String QUEUE_GROUP = "queue";
    static final Duration RETRYABLE_JOB_DELAY = Duration.ofSeconds(5);

    private final PgQueryService pgQueryService;
    private final String cronCreatePartitions;
    private final String cronDropPartitions;
    private final Clock clock;
    private final Map<QueueName, Integer> queueRetentionDays = new ConcurrentHashMap<>();
    private final Map<QueueName, Boolean> queueAutoDdl = new ConcurrentHashMap<>();
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

    public void registerQueue(QueueName queueName, int retentionDays, boolean autoDdl) throws DbqException {
        validateRetentionDays(retentionDays);
        registerQueueAutoDdl(queueName, autoDdl);
        if (autoDdl) {
            pgQueryService.createQueueTable(queueName);
        }
        var now = Instant.now(clock);
        pgQueryService.createQueuePartition(queueName, now);
        pgQueryService.createQueuePartition(queueName, now.plus(1, ChronoUnit.DAYS));
        registerRetention(queueRetentionDays, queueName, retentionDays, "queue");
    }

    public void registerSubscription(
            QueueName queueName,
            SubscriptionId subscriptionId,
            boolean historyEnabled,
            boolean serializedByKey
    ) throws DbqException {
        if (queueAutoDdl.getOrDefault(queueName, false)) {
            pgQueryService.createSubscriptionTable(queueName, subscriptionId, historyEnabled, serializedByKey);
        }

        if (historyEnabled) {
            var retentionDays = requireQueueRetention(queueName);
            var now = Instant.now(clock);
            pgQueryService.createHistoryPartition(subscriptionId, now);
            pgQueryService.createHistoryPartition(subscriptionId, now.plus(1, ChronoUnit.DAYS));
            registerRetention(historyRetentionDays, subscriptionId, retentionDays, "history");
        }
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

        for (var queueName : queueRetentionDays.keySet()) {
            pgQueryService.createQueuePartition(queueName, date);
        }
        for (var subscriptionId : historyRetentionDays.keySet()) {
            pgQueryService.createHistoryPartition(subscriptionId, date);
        }
    }

    @SneakyThrows
    public void cleanPartitions() {
        var todayUtc = LocalDate.now(clock);
        for (var entry : historyRetentionDays.entrySet()) {
            var subscriptionId = entry.getKey();
            var retentionDays = entry.getValue();
            var partitions = pgQueryService.getAllHistoryPartitions(subscriptionId);
            for (var partition : partitions) {
                if (!partition.isBefore(todayUtc.minusDays(retentionDays))) {
                    continue;
                }
                log.info("Dropping history subscription partition {} for {}", partition, subscriptionId.id());
                var result = pgQueryService.dropHistoryPartition(subscriptionId, partition);
                if (result == PgQueryService.DropPartitionResult.HAS_REFERENCES) {
                    log.warn("Partition {} for history {} still has references, skipping", partition,
                            subscriptionId.id());
                }
            }
        }

        for (var entry : queueRetentionDays.entrySet()) {
            var queueName = entry.getKey();
            var retentionDays = entry.getValue();
            var partitions = pgQueryService.getAllQueuePartitions(queueName);
            for (var partition : partitions) {
                if (!partition.isBefore(todayUtc.minusDays(retentionDays))) {
                    continue;
                }
                log.info("Dropping message partition {} for {}", partition, queueName.name());
                var result = pgQueryService.dropQueuePartition(queueName, partition);
                if (result == PgQueryService.DropPartitionResult.HAS_REFERENCES) {
                    log.warn("Partition {} for queue {} still has references, skipping", partition,
                            queueName.name());
                }
            }
        }
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

    private void validateRetentionDays(int retentionDays) {
        if (retentionDays <= 0) {
            throw new IllegalArgumentException("retentionDays must be > 0");
        }
    }

    private int requireQueueRetention(QueueName queueName) {
        var retentionDays = queueRetentionDays.get(queueName);
        if (retentionDays == null) {
            throw new IllegalStateException("Queue %s is not registered. Register queue retention first."
                    .formatted(queueName));
        }
        return retentionDays;
    }

    private void registerQueueAutoDdl(QueueName queueName, boolean autoDdl) {
        var existingAutoDdl = queueAutoDdl.putIfAbsent(queueName, autoDdl);
        if (existingAutoDdl != null && existingAutoDdl != autoDdl) {
            throw new IllegalArgumentException(
                    "Conflicting autoDdl for queue %s: existing=%s, requested=%s"
                            .formatted(queueName, existingAutoDdl, autoDdl));
        }
    }

    private <T> void registerRetention(Map<T, Integer> retentions, T key, int retentionDays, String target) {
        var existingRetention = retentions.putIfAbsent(key, retentionDays);
        if (existingRetention != null && existingRetention != retentionDays) {
            throw new IllegalArgumentException(
                    "Conflicting retentionDays for %s %s: existing=%s, requested=%s"
                            .formatted(target, key, existingRetention, retentionDays));
        }
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

    @FunctionalInterface
    interface ThrowingJob {
        void accept(PgTableManager tableManager) throws Exception;
    }

    @FunctionalInterface
    interface Sleeper {
        void sleep(Duration duration) throws InterruptedException;
    }

    private static void doJob(JobExecutionContext context, ThrowingJob job) {
        try {
            var tableManager = (PgTableManager) context.getScheduler().getContext().get(TABLE_MANAGER);
            doJob(tableManager, job, RETRYABLE_JOB_DELAY, duration -> Thread.sleep(duration.toMillis()));
        } catch (SchedulerException e) {
            log.error("Unpredicted exception during getting table manager", e);
            throw new IllegalArgumentException(e);
        } catch (InterruptedException e) {
            log.error("Interrupted exception during job execution", e);
            Thread.currentThread().interrupt();
        }
    }

    static void doJob(
            PgTableManager tableManager,
            ThrowingJob job,
            Duration retryDelay,
            Sleeper sleeper
    ) throws InterruptedException {
        do {
            try {
                job.accept(tableManager);
                break;
            } catch (RetryablePersistenceException e) {
                log.warn("Retryable persistence exception during job execution", e);
                sleeper.sleep(retryDelay);
            } catch (Exception e) {
                log.error("Unpredicted exception during job execution", e);
                break;
            }
        } while (true);
    }
}
