# Verified Hypotheses

## 2026-04-08

### Hypothesis
Rewriting `PgQueryService.selectMessages` from `join queue first` to `lock rows from subscription first in a CTE, then join queue` will improve polling performance more than further index tuning.

### Setup
- Local PostgreSQL 18.3 in Docker on `localhost:55432`
- Schema: `dbq_test`
- Tables: `test_message`, `test_subscription_one`
- Dataset:
  - `1,000,000` rows in `test_message`
  - `1,000,000` rows in `test_subscription_one`
  - about `904k` rows ready by `execute_after`
- Measurement:
  - `EXPLAIN (ANALYZE, BUFFERS)`
  - compared old query and CTE rewrite
  - tested `LIMIT 100` and `LIMIT 1000`

### Result
Hypothesis rejected.

On the warmed dataset the original query was faster:
- `LIMIT 100`: old `0.42 ms`, new `1.56 ms`
- `LIMIT 1000`: old `5.46 ms`, new `9.44 ms`

### Conclusion
Do not implement this rewrite in `PgQueryService` based on the current evidence.

PostgreSQL already builds the useful plan shape for the original SQL:
- index scan on `subscription.execute_after`
- nested loop
- point lookup into `queue` by primary key

The CTE version adds overhead from materialization and an extra sort without improving the access path on this dataset.
