# ADR 0001: Prefer `key_lock` Over Logical Partitions for Per-Key Serialization

## Status

Accepted

## Context

DBQ needs a way to process messages sequentially by `key` while preserving the current runtime model:

- no more than one active handler for the same `key` across the whole cluster
- coordination between multiple pods through PostgreSQL
- keep the current queue table as the shared source of messages
- keep subscription live tables as the polling source
- keep retry, fail, complete, and optional history behavior unchanged

The goal is to add per-key serialization with minimal architectural disruption.

## Decision

DBQ will not introduce logical partitions for this problem.

Instead, DBQ uses a subscription-scoped `key_lock` table together with a `key` column stored in the live
subscription table.

The preferred model is:

1. `subscription` stores its own `key`
2. each active `key` has a row in `<subscription>_key_lock`
3. insert into the subscription path also ensures the lock row exists
4. polling joins `subscription` with `key_lock`
5. polling uses `FOR UPDATE SKIP LOCKED` on both live rows and lock rows
6. if another consumer already holds the row lock for the same `key`, that key is skipped
7. on `complete` and `fail`, the `key_lock` row is deleted when no more live rows remain for that `key`

Readiness remains defined by `subscription.execute_after`.

## Alternatives Considered

### Advisory lock by key

Idea:

- use `pg_try_advisory_xact_lock(subscription, hash(key))`

Rejected because:

- the lock is acquired during handling, not during ordered selection
- batching messages for the same `key` becomes awkward
- the model is weaker for ordered stream processing where `key` is the scheduling unit

### Logical partitions

Idea:

- use a fixed number of partitions per queue
- route each message by `hash(key) % maxPartitions`
- poll by partition

Pros:

- bounded concurrency
- simple cluster coordination through advisory lock
- familiar analogy with Kafka partitions

Rejected because:

- ordering is guaranteed only within a partition, not exactly per `key`
- it introduces a routing layer and `maxPartitions` as a new public concern
- polling becomes more complex because consumers must search candidate partitions, not just ready rows
- it is a much heavier change to the DBQ model than required for per-key serialization

## Rationale

`key_lock` is a closer fit to the actual requirement than logical partitions.

It preserves the current DBQ architecture:

- queue table remains the shared source of message payloads
- subscription live table remains the scheduler state
- retry and failure handling still operate on subscription rows
- optional history remains unchanged

It also provides stronger semantics for the target use case:

- serialization follows the actual business `key`
- no coarse-grained partitioning is introduced
- no partition scheduler is required
- concurrency control is explicit and local to a subscription

## Consequences

### Positive

- exact per-key mutual exclusion instead of partition-level approximation
- smaller change surface in DDL, polling, and completion logic
- no new user-facing concept like `maxPartitions`
- compatible with current retry/history model

### Negative

- `key_lock` does not improve polling performance by itself
- cleanup logic is required on terminal transitions
- long-running handlers hold the lock longer for the same `key`
- `maxPollRecords > 1` increases lock hold time and must be used deliberately

## Operational Notes

- safe baseline for long-running processing is `maxPollRecords = 1`
- readiness is still controlled only by `subscription.execute_after`
- retention and live-table cleanup must not leave orphaned `key_lock` rows

## Implementation Notes

Expected polling shape:

```sql
SELECT ...
FROM subscription s
JOIN queue q ON q.id = s.message_id
    AND q.originated_at = s.originated_at
JOIN subscription_key_lock k ON k.key = s.key
WHERE s.execute_after < CURRENT_TIMESTAMP
ORDER BY s.execute_after, s.id
LIMIT :maxPollRecords
FOR UPDATE OF k, s SKIP LOCKED
```

Key points:

- `subscription` remains the source of readiness
- `queue` still provides payload and headers
- `key_lock` is only a coordination table
- cleanup should delete a lock row only when no live subscription rows remain for that `key`
