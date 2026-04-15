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

Readiness по-прежнему определяется по `subscription.execute_after`.

### Базовая идея

1. Для каждого `key`, который есть в pending сообщениях subscription, существует row в `key_lock` table.
2. При вставке нового сообщения делается `upsert` в `key_lock`.
3. Во время polling запрос делает `join` с `key_lock` и использует `FOR UPDATE SKIP LOCKED`.
4. Если другой consumer уже держит lock на этом `key`, сообщения этого `key` просто пропускаются.
5. После `complete/fail` row из `key_lock` удаляется, если в subscription больше не осталось сообщений с таким `key`.

### Пример polling flow

```sql
SELECT ...
FROM subscription s
JOIN queue q ON q.id = s.message_id
    AND q.originated_at = s.originated_at
JOIN subscription_key_lock k ON k.key = q.key
WHERE s.execute_after < CURRENT_TIMESTAMP
ORDER BY s.execute_after, s.id
LIMIT :maxPollRecords
FOR UPDATE OF k, s SKIP LOCKED
```

Смысл этого запроса:

- `subscription` определяет, какие сообщения ready
- `queue` даёт payload и `key`
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

Идея неполная без отдельного batch handling rules.

Нужно явно зафиксировать:

- допустимо, что один poll возвращает несколько сообщений одного `key`
- сообщения внутри poll обрабатываются в порядке `execute_after`, затем `id`
- если первое сообщение этого `key` уходит в `retry` или блокирует обработку, оставшиеся уже выбранные сообщения того же `key`
  нельзя продолжать обрабатывать как независимые

То есть для этого варианта понадобится доработка `Consumer`, а не только SQL/DDL изменения.

### Cleanup

`key_lock` row нужно удалять на `complete/fail`, если в subscription больше не осталось сообщений с тем же `key`.

Идея cleanup:

- `delete from key_lock where key = ? and not exists (...)`
- проверка делается по live subscription rows
- retention cleanup должен быть совместим с этой логикой и не оставлять orphan lock rows

### Открытые вопросы

- какую именно схему дать `key_lock` table кроме самого `key`
- нужен ли `updated_at` или достаточно только primary key
- как лучше реализовать cleanup при `complete/fail`, чтобы не делать лишние expensive checks
- как должен вести себя `Consumer`, если в одном batch уже выбраны несколько сообщений одного `key`
- нужны ли отдельные индексы, чтобы join с `key_lock` не ухудшил polling path
 

## Priority queue

Нужно добавить в DBQ механизм приоритетной очереди на базе текущей модели DBQ.
