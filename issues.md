# Issues
1. Проанализировать ещё раз пакеты core
2. Подумать нужен ли DuplicateKeyException, как его может использовать клиент
3. Подумать нужен ли RetrayablePersistenceException, NonRetrayablePersistenceException
4. Подумать нужен ли SerializerException
5. Подумать нужен ли PartitionHasReferencesException
6. Подумать нужен ли MissingPartitionException

## Notes

- Verified with `mvn -q -pl core test`.
- Verified with `mvn -q -pl integration-tests -am test`.
