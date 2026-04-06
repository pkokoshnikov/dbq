# Feature: Logical Partitions

## 1. Goal

Нужно добавить в DBQ механизм логических партиций, похожий по смыслу на Kafka partitions, но встроенный в текущую PostgreSQL-модель DBQ.

Фича должна дать:

- обработку одной партиции не более чем в одном потоке во всём кластере
- ограничение максимального числа партиций для очереди
- синхронизацию между несколькими pod-ами через PostgreSQL

При этом фича должна как можно меньше ломать текущую архитектуру:

- queue table остаётся общим источником сообщений
- subscription live table остаётся источником данных для consumer polling
- retry / fail / complete / history продолжают работать как сейчас

## 2. Problem Statement

Сейчас DBQ умеет параллельно обрабатывать сообщения через несколько worker threads и несколько pod-ов, но не умеет:

- закреплять одно сообщение за логической партицией
- гарантировать, что сообщения одной партиции обрабатываются последовательно
- распределять партиции между несколькими инстансами приложения

Из-за этого нет аналога kafka-like ordering внутри partition.

## 3. High-Level Idea

Нужно ввести логическую партицию как отдельное свойство сообщения.

Базовая модель:

- у очереди есть `maxPartitions`
- при публикации каждое сообщение получает `partition`
- `partition` вычисляется детерминированно
- consumer перед polling пытается занять одну партицию через PostgreSQL advisory lock
- после захвата worker читает и обрабатывает только сообщения этой партиции

Таким образом:

- одна партиция одновременно обрабатывается только одним worker
- разные партиции могут обрабатываться параллельно
- координация между pod-ами идёт через БД

## 4. Functional Requirements

### FR-1. Queue-level partition configuration

Партиционирование должно быть свойством очереди.

Нужно расширить `QueueConfig` параметром:

- `maxPartitions`

Требования:

- `maxPartitions` задаётся при `registerQueue(...)`
- значение обязательно должно быть `> 0`
- если queue уже зарегистрирована, повторная регистрация с другим `maxPartitions` должна приводить к ошибке

### FR-2. Deterministic message-to-partition mapping

Каждое сообщение должно попадать в одну и только одну логическую партицию.

Требования:

- partition должен вычисляться детерминированно
- одинаковый вход должен давать одинаковую партицию
- результат должен быть в диапазоне `[0, maxPartitions - 1]`

Базовое предположение для первой версии:

- partition вычисляется из `message.key`

Открытый вопрос на будущее:

- нужен ли отдельный `partitionKey`, отличный от dedup key

### FR-3. Single-thread processing per partition

В один момент времени одну логическую партицию может обрабатывать только один worker во всём кластере.

Требования:

- это должно работать при нескольких потоках в одном pod
- это должно работать при нескольких pod-ах
- конкурентный доступ должен синхронизироваться через PostgreSQL

### FR-4. Database-based synchronization

Для координации нужно использовать PostgreSQL advisory locks.

Требования:

- lock должен захватываться до `poolAndProcess`
- если lock на партицию не взят, worker должен пробовать другую партицию
- lock должен освобождаться автоматически при завершении транзакции

Предпочтительный вариант:

- использовать `pg_try_advisory_xact_lock(...)`

Почему:

- lock живёт в рамках транзакции
- не нужен отдельный heartbeat
- не нужен отдельный lease table
- lock не утечёт после rollback/connection close

### FR-5. Partition-aware polling

После захвата партиции consumer должен читать только сообщения этой партиции.

Требования:

- `selectMessages(...)` должен уметь фильтровать по partition
- выборка должна сохранять текущую модель:
  - `execute_after < CURRENT_TIMESTAMP`
  - `ORDER BY execute_after`
  - `FOR UPDATE SKIP LOCKED`

### FR-6. Ordering guarantee inside one partition

Для сообщений одной логической партиции нужен детерминированный порядок обработки.

Минимальное требование для первой версии:

- порядок внутри партиции должен быть стабилен и предсказуем

Предпочтительный порядок:

- сначала по `execute_after`
- затем по `created_at`
- затем по `id`

Это нужно, чтобы избежать неявного reorder при одинаковом `execute_after`.

### FR-7. Compatibility with retries

Retry не должен ломать модель партиций.

Требования:

- сообщение при retry остаётся в своей исходной партиции
- повторная обработка выполняется в той же логической партиции
- retry не должен позволять второму worker обработать другую запись той же партиции параллельно с текущей

### FR-8. Compatibility with history

History mode должен продолжать работать.

Требования:

- если `historyEnabled = true`, информация о partition должна сохраняться достаточно, чтобы поведение было диагностируемым
- если `historyEnabled = false`, отсутствие history не должно влиять на partition processing

Открытый вопрос:

- хранить ли `partition` в history table как отдельное поле

Для первой версии это желательно.

## 5. Non-Functional Requirements

### NFR-1. Minimal architectural changes

Первая версия должна минимально менять существующую архитектуру DBQ.

Предпочтительно:

- не отказываться от subscription live table
- не переходить на cursor/offset model
- не вводить lease table

### NFR-2. Horizontal scalability

Решение должно работать на нескольких pod-ах без внешнего coordinator.

Требования:

- только PostgreSQL как coordination mechanism
- без Redis, ZooKeeper, Kafka coordinator и т.д.

### NFR-3. Graceful failure behavior

Если pod или поток падает:

- advisory lock должен освобождаться автоматически
- другая нода должна иметь возможность продолжить обработку партиции

### NFR-4. Bounded overhead

Решение не должно вносить чрезмерную нагрузку на polling loop.

Нужно учитывать:

- стоимость попыток захвата advisory lock
- стоимость перебора партиций
- стоимость SQL-фильтрации по partition

## 6. Preferred Implementation Direction

На текущий момент предпочтителен такой подход.

### Data model changes

Нужно добавить `partition`:

- в queue table
- в subscription live table
- желательно в history table

### Queue config changes

Нужно добавить в `QueueConfig.Properties`:

- `maxPartitions`

Дефолт первой версии:

- `1`

То есть очередь без явной настройки ведёт себя как today-compatible single partition queue.

### Producer behavior

Producer должен:

1. вычислить partition
2. записать его в queue row
3. trigger должен перенести partition в subscription live row

### Consumer behavior

Consumer worker должен:

1. выбрать candidate partition
2. попытаться взять advisory xact lock
3. если lock получен, выполнить polling только по этой partition
4. обработать до `maxPollRecords` сообщений только этой partition
5. завершить транзакцию и освободить lock

## 7. Constraints and Assumptions

### C-1. Partition count is bounded

Решение предполагает, что `maxPartitions` не бесконечен и не исчисляется тысячами для обычной очереди.

Иначе тупой перебор partition id в каждом worker станет дорогим.

### C-2. Partition ownership is transactional, not sticky

Для первой версии не требуется стабильное долговременное владение партицией одним pod-ом.

Допускается, что:

- в одной транзакции партицию обрабатывает pod A
- в следующей транзакции ту же партицию возьмёт pod B

Главное требование:

- одновременно не более одного owner на партицию

### C-3. This is not a full Kafka clone

Первая версия не должна пытаться повторить:

- offsets
- consumer rebalance protocol
- persistent partition ownership metadata
- replay model как у Kafka

Цель только в том, чтобы получить:

- partition-based ordering
- partition-based concurrency control

## 8. Open Design Questions

### Q-1. Partition source

От чего считать partition:

- от `message.key`
- от отдельного `partitionKey`
- от пользовательской стратегии

Предпочтение для первой версии:

- от `message.key`

### Q-2. Candidate partition selection

Как worker выбирает следующую партицию:

- round-robin
- random
- sticky preferred partition + fallback

Предпочтение для первой версии:

- sticky preferred partition + round-robin fallback

### Q-3. Batch processing semantics

Если worker держит partition lock, сколько сообщений он обрабатывает за одну транзакцию:

- одно
- до `maxPollRecords`
- до исчерпания очереди по partition

Предпочтение для первой версии:

- до `maxPollRecords`

### Q-4. Partition in history table

Нужно ли явно хранить `partition` в history table.

Предпочтение:

- да, для дебага и аналитики

## 9. Explicitly Out of Scope for V1

В первую версию не входят:

- перераспределение партиций через отдельный coordinator
- lease table
- sticky ownership на минуты/секунды
- offset-based storage model
- priority-aware partition balancing
- динамическое изменение `maxPartitions` для уже существующей очереди

## 10. Acceptance Criteria

Фича считается реализованной, если:

1. очередь можно создать с `maxPartitions > 1`
2. producer стабильно вычисляет partition для сообщения
3. два worker-а из одного или разных pod-ов не могут одновременно обрабатывать одну и ту же partition
4. разные partition могут обрабатываться параллельно
5. retry / fail / complete продолжают работать
6. при падении worker lock освобождается транзакционно
7. интеграционные тесты подтверждают ordering и exclusivity

## 11. Recommended Testing Scenarios

Нужно покрыть минимум такие сценарии.

### T-1. Same partition, same subscription

- публикуем несколько сообщений одной partition
- запускаем несколько worker-ов
- проверяем, что обработка идёт последовательно

### T-2. Different partitions in parallel

- публикуем сообщения в разные partition
- запускаем несколько worker-ов
- проверяем, что есть реальный parallel processing

### T-3. Multi-pod exclusivity

- два consumer instance на одну subscription
- одна partition не обрабатывается одновременно двумя инстансами

### T-4. Retry behavior

- сообщение падает retryable exception
- остаётся в той же partition
- повторно обрабатывается корректно

### T-5. History mode

- `historyEnabled = true`
- partition-aware flow работает и при complete, и при fail

### T-6. Advisory lock release on transaction end

- lock не залипает после commit / rollback / stop consumer

