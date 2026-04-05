# Issues
1. `PgTableManager.cleanPartitions()` использует `LocalDate.now()` в локальной timezone JVM, а queue/history partitions именуются и создаются по UTC.
Около границы суток это может удалить партицию слишком рано или слишком поздно относительно реального UTC retention.

2. `PgTableManager.registerSubscription(queueName, subscriptionId, retentionDays)` по умолчанию вызывает overload с `historyEnabled = false` и фактически ничего не делает для history cleanup.
Это легко пропустить при включённой history и получить бесконечный рост history partitions, если использовать старую перегрузку по привычке.

## Notes

- Verified with `mvn -q -pl core test`.
- Verified with `mvn -q -pl integration-tests -am test`.
