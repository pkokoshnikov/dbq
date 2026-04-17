# Test Coverage Recommendations

Список кейсов, которые выглядят наиболее важными по `core/target/site/jacoco` и пока не закрыты тестами достаточно хорошо.

## 1. `AbstractConsumer.poolLoop`

Файл: `core/src/main/java/org/pak/dbq/internal/consumer/AbstractConsumer.java`

Что важно покрыть:
- `MessageSerializationException` должен останавливать consumer.
- `NonRetrayablePersistenceException` должен останавливать consumer.
- `RetryablePersistenceException` должен ставить паузу `persistenceExceptionPause`.
- Неожиданные исключения должны ставить паузу `unpredictedExceptionPause`.
- `InterruptedException` в `poolLoop` должен корректно завершать цикл и выставлять `Thread.currentThread().interrupt()`.

Почему это важно:
- Это основной recovery loop consumer’а.
- Ошибка здесь может дать либо вечный restart loop, либо молчаливую остановку, либо зависание без паузы.

## 2. `QueueManager` rollback on registration failure

Файл: `core/src/main/java/org/pak/dbq/api/QueueManager.java`

Что важно покрыть:
- `registerQueue(...)` должен откатывать `queueConfigs`, если `tableManager.registerQueue(...)` падает.
- `registerConsumer(...)` должен откатывать `consumerConfigs`, если `registerSubscription(...)` или создание `ConsumerStarter` падает.
- Повторная регистрация после такого падения должна проходить как новая.

Почему это важно:
- Сейчас в коде есть явный rollback через `remove(...)`.
- Без теста легко получить частично зарегистрированную очередь или consumer, который потом нельзя восстановить.

## 3. `ConsumerStarter.stop()`

Файл: `core/src/main/java/org/pak/dbq/internal/consumer/ConsumerStarter.java`

Что важно покрыть:
- Успешный `awaitTermination(...)` приводит к штатной остановке.
- Timeout приводит к `shutdownNow()`.
- `InterruptedException` в `awaitTermination(...)` должен восстанавливать interrupt flag.

Почему это важно:
- Это путь корректного shutdown-а, особенно если consumer живет внутри приложения с graceful stop.

## 4. `RecordingBatchAcknowledger`

Файл: `core/src/main/java/org/pak/dbq/internal/consumer/RecordingBatchAcknowledger.java`

Что важно покрыть:
- `retry(..., null duration, ...)` и negative duration должны падать.
- `retry(..., null exception)` должен падать.
- `fail(..., null exception)` должен падать.
- `complete/retry/fail` на record не из batch должен падать.
- Повторная acknowledgment одного record должна падать.

Почему это важно:
- Это публичный контракт batch acknowledger’а.
- Ошибки здесь приводят к неконсистентному batch outcome.

## 5. `Consumer.handleRetryableException(...)`

Файл: `core/src/main/java/org/pak/dbq/internal/consumer/Consumer.java`

Что важно покрыть:
- Если `retryablePolicy` возвращает `Optional.empty()`, message должен уйти в `fail`.
- Гвард `messageContainer.getAttempt().equals(Integer.MAX_VALUE - 1)` должен форсировать `fail`, даже если policy вернул delay.

Почему это важно:
- Это boundary-case для переполнения attempt counter.
- Без теста можно случайно сломать защиту и получить некорректный retry цикл.

## Что уже хорошо покрыто

- `QueueManager` happy path и конфликты регистрации.
- `Consumer` основные сценарии `complete`, `retry`, `fail`, `blocking`.
- `BatchConsumer` missing outcome, duplicate outcome и fallback после ошибки в acknowledger.

## Следующий шаг

Если идти по приоритету, я бы сначала добавил тесты на:
1. `QueueManager` rollback.
2. `AbstractConsumer.poolLoop`.
3. `ConsumerStarter.stop()`.
