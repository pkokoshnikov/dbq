# Ideas

## Logical Partitions

Нужно добавить в DBQ механизм последовательной обработки сообщений по `key`, встроенный в текущую модель DBQ.

Фича должна дать:

- обработку одного `key` не более чем в одном потоке во всём кластере
- синхронизацию между несколькими pod-ами через PostgreSQL

При этом фича должна как можно меньше ломать текущую архитектуру:

- queue table остаётся общим источником сообщений
- subscription live table остаётся источником данных для consumer polling
- retry / fail / complete / history продолжают работать как сейчас

### Альтернативные решения

#### `lock by key` через advisory lock

Идея: использовать `pg_try_advisory_xact_lock(subscription, hash(key))`.

Почему не выбрали:

- lock берётся на этапе обработки, а не на этапе упорядоченного `select`
- неудобно вычитывать batch сообщений одного `key`
- модель плохо описывает ordered stream processing, где `key` должен быть единицей scheduling

#### `logical partitions`

Идея: у queue фиксированное число partitions, сообщение маршрутизируется по `hash(key) % maxPartitions`, polling идёт по partition.

Плюсы:

- bounded concurrency
- простая кластерная координация через advisory lock
- хорошая аналогия с Kafka partitions

Почему не выбрали:

- порядок гарантируется только внутри partition, а не точно по `key`
- нужен отдельный routing layer с `maxPartitions`
- polling получается сложнее: нужно искать candidate partitions, а не просто ready messages
- это более тяжёлое изменение модели DBQ, чем требуется для задачи per-key serialization

### Вывод

Сейчас предпочтительный вариант для DBQ: не `logical partitions`, а `key_lock` table для сериализации обработки по `key`.

### Предпочтительный дизайн

Нужна дополнительная таблица блокировок на уровне subscription, например `<subscription>_key_lock`.

Эта таблица:

- не хранит scheduler state
- не участвует в выборе readiness
- используется только как носитель row-level lock для `key`
- имеет минимальную схему: один `primary key (key)` без дополнительных колонок

`key` при этом хранится прямо в `subscription`, чтобы polling и cleanup не зависели от join в `queue` только ради получения ключа.

Readiness по-прежнему определяется по `subscription.execute_after`.

### Базовая идея

1. `subscription` хранит собственную колонку `key`.
2. Для каждого `key`, который есть в pending сообщениях subscription, существует row в `key_lock` table.
3. При вставке нового сообщения делается `upsert` в `key_lock`.
4. При вставке в `subscription` туда же записывается `key`.
5. Во время polling запрос делает `join` с `key_lock` и использует `FOR UPDATE SKIP LOCKED`.
6. Если другой consumer уже держит lock на этом `key`, сообщения этого `key` просто пропускаются.
7. После `complete/fail` row из `key_lock` удаляется, если в subscription больше не осталось сообщений с таким `key`.

### Пример polling flow

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

Смысл этого запроса:

- `subscription` определяет, какие сообщения ready
- `queue` даёт payload
- `subscription` даёт `key`
- `key_lock` не даёт двум consumer-ам одновременно взять один и тот же `key`

### Что важно в этом варианте

- не нужен fixed `maxPartitions`
- сериализация идёт по реальному `key`, а не по coarse-grained partition
- не нужен отдельный partition scheduler
- `key_lock` table остаётся простой и используется только для coordination
- клиент может контролировать длительность удержания lock через `maxPollRecords`

### Ограничения и последствия

- `key_lock` table сама по себе не ускоряет polling, а только даёт per-key mutual exclusion
- readiness остаётся на `subscription.execute_after`
- для длинных обработок безопасный baseline режим это `maxPollRecords = 1`
- при `maxPollRecords > 1` клиент осознанно обменивает более долгие lock-и на throughput

### Batch handling

Для удобства пользователя batch handling лучше сделать не как внутренний grouping layer, а как отдельный режим API рядом с обычным
single-message handler.

Предполагаемый подход:

- сохранить текущий `MessageHandler<T>` для простого случая
- добавить отдельный `BatchMessageHandler<T>` для batch processing
- в batch handler передавать список `MessageRecord<T>` и отдельный acknowledger interface

Идея API:

```java
public interface BatchMessageHandler<T> {
    void handle(List<MessageRecord<T>> messages, BatchAcknowledger<T> acknowledger) throws Exception;
}
```

Где `BatchAcknowledger<T>` умеет:

- `complete(record)`
- `retry(record, duration, exception)`
- `fail(record, exception)`

На текущем этапе batch acknowledger лучше оставить минимальным:

- без `retry(record, exception)`
- без встроенного вызова `RetryablePolicy`
- если нужен policy-driven retry, внешний код сначала вызывает `RetryablePolicy.apply(...)`, а потом выбирает между `retry(...)` и `fail(...)`

### Batch contract

Сейчас базовый контракт для batch mode выглядит так:

- пользователь получает всю пачку и сам решает, как её группировать и обрабатывать
- framework не навязывает grouping policy и не валидирует ordering внутри одной группы
- framework требует strict mode: каждое сообщение из batch должно получить ровно один outcome
- если handler завершился, а часть batch осталась без `complete/retry/fail`, это считается ошибкой
- bulk convenience methods можно добавить позже поверх базового per-message API

### `MessageRecord`

Для batch mode лучше использовать не голый `Message<T>`, а отдельный envelope `MessageRecord<T>`.

Это нужно чтобы:

- у handler был user-facing объект с payload, key, headers и metadata
- acknowledger работал по стабильной identity конкретного сообщения
- одинаковые payload/messages не путались между собой

Иными словами, `MessageRecord<T>` нужен как carrier между polling layer и batch acknowledger.

### Cleanup

`key_lock` row нужно удалять на `complete/fail`, если в subscription больше не осталось сообщений с тем же `key`.

Идея cleanup:

- cleanup на `complete/fail` пока считается базовым вариантом
- `delete from key_lock where key = ? and not exists (...)`
- проверка делается только по live subscription rows и не требует join в `queue`
- retention cleanup должен быть совместим с этой логикой и не оставлять orphan lock rows

### Индексы

Базовые предположения по индексам сейчас такие:

- отдельный индекс на `key_lock.key` не нужен, если `key` уже является `primary key`
- дополнительные индексы нужно подбирать на `subscription` / `queue` join path уже с учётом `subscription.key`

### Открытые вопросы

- нужен ли cleanup на каждом `complete/fail` всегда, или позже захочется добавить более ленивую стратегию
- какие именно дополнительные индексы нужны на `subscription` / `queue` join path, теперь уже с учётом `subscription.key`
- какой именно набор полей должен входить в `MessageRecord<T>`
 

## Priority queue

Нужно добавить в DBQ механизм приоритетной очереди на базе текущей модели DBQ.
