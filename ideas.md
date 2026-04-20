# Ideas

## Logical Partitions

Итог по этому вопросу вынесен в ADR:

- [ADR 0001: Prefer `key_lock` Over Logical Partitions for Per-Key Serialization](adr/0001-key-lock-over-logical-partitions.md)

Короткая версия решения:

- `logical partitions` не выбраны для DBQ
- предпочтён `key_lock` table на уровне subscription
- сериализация должна идти по реальному `key`, а не по coarse-grained partition
- readiness остаётся на `subscription.execute_after`

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

## Refactoring PgQueryService

Я хочу сделать следующее, мне нужно разделить PgQueryService так что чтобы убрать проверки if, возможно добавить стратегии по созданию таблиц, подписок,
публикации сообщений, потреблению сообщений, комплиту, фейлу 
