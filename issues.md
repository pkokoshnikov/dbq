# Issues
1. Нет валидации `ConsumerConfig.Properties`.
Решение: валидировать `concurrency > 0`, `maxPollRecords > 0`, паузы `>= 0` в конструкторе/билдере и падать сразу с понятной ошибкой.

2. `ProducerConfig.Properties.retentionDays` и `ConsumerConfig.Properties.retentionDays` сейчас не влияют на поведение.
Решение: прокинуть их в интеграцию с `PgTableManager`.

## Notes

- Verified with `mvn -q -pl core test`.
- Verified with `mvn -q -pl integration-tests -am test`.
