# Issues

## 1. `core` does not compile reliably on the current JDK/Maven setup

- Severity: High
- Status: Confirmed by running `mvn -q -pl core test`
- Files:
  - `pom.xml`
  - `core/pom.xml`
- Problem:
  - The build fails with `cannot find symbol` for Lombok-generated getters and `log` fields.
  - Lombok is present only as a dependency, but there is no explicit compiler/toolchain configuration for annotation processing on the current JDK.
- Impact:
  - The main module cannot be built or tested in the current environment.

## 2. `QueueAdapter` does not manage partition cron jobs

- Severity: High
- Files:
  - `core/src/main/java/org/pak/messagebus/core/QueueAdapter.java`
  - `core/src/main/java/org/pak/messagebus/core/TableManager.java`
- Problem:
  - `MessageBus.startSubscribers()` starts partition cron jobs and `stopSubscribers()` stops them.
  - `QueueAdapter` starts/stops only subscribers and never starts/stops `TableManager` cron jobs.
- Impact:
  - Automatic partition creation/cleanup is disabled for queue adapter scenarios.
  - Old partitions may accumulate indefinitely.

## 3. `TableManager.stopCronJobs()` does not shut down Quartz

- Severity: Medium
- File:
  - `core/src/main/java/org/pak/messagebus/core/TableManager.java`
- Problem:
  - `stopCronJobs()` calls `scheduler.clear()` but does not call `shutdown()`.
  - This removes jobs but leaves the Quartz scheduler itself alive.
- Impact:
  - Scheduler threads can leak.
  - Lifecycle handling is incomplete and can cause resource issues on restart/shutdown.

## 4. Subscribers cannot be restarted after stop

- Severity: Medium
- File:
  - `core/src/main/java/org/pak/messagebus/core/MessageProcessorStarter.java`
- Problem:
  - `stop()` calls `fixedThreadPoolExecutor.shutdown()`.
  - `start()` later tries to reuse the same executor instance.
- Impact:
  - A repeated `startSubscribers()` after `stopSubscribers()` can fail with `RejectedExecutionException`.
  - The subscriber lifecycle is effectively one-shot.

## Notes

- `spring`, `pg`, and `integration-tests` were not fully built end-to-end in the sandbox.
- Reasons:
  - Maven inside the sandbox cannot write to the default `~/.m2` location for some module builds.
  - Using a temporary repo in `/tmp` then fails because network access to Maven Central is restricted in the sandbox.
- The `core` compilation problem above was still confirmed directly with a local Maven run.
