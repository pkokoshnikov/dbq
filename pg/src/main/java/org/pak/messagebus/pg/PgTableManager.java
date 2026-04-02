package org.pak.messagebus.pg;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.pak.messagebus.core.MessageName;
import org.pak.messagebus.core.SubscriptionName;
import org.pak.messagebus.core.error.PartitionHasReferencesException;
import org.pak.messagebus.core.error.RetrayablePersistenceException;
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
    public static final String MESSAGE_BUS_GROUP = "message-bus";

    private final PgQueryService pgQueryService;
    private final String cronCreatePartitions;
    private final String cronDropPartitions;
    private final Map<MessageName, Integer> messageNameStorageDays = new ConcurrentHashMap<>();
    private final Map<SubscriptionName, Integer> historyStorageDays = new ConcurrentHashMap<>();
    private Scheduler scheduler;

    public PgTableManager(PgQueryService pgQueryService, String cronCreatePartitions, String cronDropPartitions) {
        this.pgQueryService = pgQueryService;
        this.cronCreatePartitions = cronCreatePartitions;
        this.cronDropPartitions = cronDropPartitions;
    }

    public void registerMessage(MessageName messageName, int storageDays) {
        pgQueryService.initMessageTable(messageName);
        pgQueryService.createMessagePartition(messageName, Instant.now());
        pgQueryService.createMessagePartition(messageName, Instant.now().plus(1, ChronoUnit.DAYS));
        messageNameStorageDays.putIfAbsent(messageName, storageDays);
    }

    public void registerSubscription(MessageName messageName, SubscriptionName subscriptionName, int storageDays) {
        pgQueryService.initMessageTable(messageName);
        pgQueryService.initSubscriptionTable(messageName, subscriptionName);
        pgQueryService.createHistoryPartition(subscriptionName, Instant.now());
        pgQueryService.createHistoryPartition(subscriptionName, Instant.now().plus(1, ChronoUnit.DAYS));
        historyStorageDays.putIfAbsent(subscriptionName, storageDays);
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

        messageNameStorageDays.keySet().forEach(messageName -> pgQueryService.createMessagePartition(messageName, date));
        historyStorageDays.keySet().forEach(subscriptionName -> pgQueryService.createHistoryPartition(subscriptionName, date));
    }

    public void cleanPartitions() {
        historyStorageDays.forEach((subscriptionName, storageDays) -> {
            var partitions = pgQueryService.getAllHistoryPartitions(subscriptionName);
            partitions.stream()
                    .filter(partition -> partition.isBefore(LocalDate.now().minusDays(storageDays)))
                    .forEach(partition -> {
                        try {
                            log.info("Dropping history subscription partition {} for {}", partition,
                                    subscriptionName.name());
                            pgQueryService.dropHistoryPartition(subscriptionName, partition);
                        } catch (PartitionHasReferencesException e) {
                            log.warn("Partition {} for history {} still has references, skipping", partition,
                                    subscriptionName.name());
                        }
                    });
        });

        messageNameStorageDays.forEach((messageName, storageDays) -> {
            var partitions = pgQueryService.getAllMessagePartitions(messageName);
            partitions.stream()
                    .filter(partition -> partition.isBefore(LocalDate.now().minusDays(storageDays)))
                    .forEach(partition -> {
                        try {
                            log.info("Dropping history partition {} for {}", partition, messageName.name());
                            pgQueryService.dropMessagePartition(messageName, partition);
                        } catch (PartitionHasReferencesException e) {
                            log.warn("Partition {} for message {} still has references, skipping", partition,
                                    messageName.name());
                        }
                    });
        });
    }

    private void addCronJob(String jobKey, Class<? extends Job> clazz, String cron, Scheduler scheduler)
            throws SchedulerException {
        var key = new JobKey(jobKey, MESSAGE_BUS_GROUP);
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
                } catch (RetrayablePersistenceException e) {
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
