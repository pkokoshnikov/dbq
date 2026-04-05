# Issues
1. `ProducerConfig.Properties.retentionDays` и `ConsumerConfig.Properties.retentionDays` сейчас не влияют на поведение.
Решение: прокинуть их в интеграцию с `PgTableManager`.

## Notes

- Verified with `mvn -q -pl core test`.
- Verified with `mvn -q -pl integration-tests -am test`.
