# Ideas

## Logical Partitions

Нужно добавить в DBQ механизм логических партиций, похожий по смыслу на Kafka partitions, но встроенный в текущую модель DBQ.

Фича должна дать:

- обработку одной партиции не более чем в одном потоке во всём кластере
- синхронизацию между несколькими pod-ами через PostgreSQL

При этом фича должна как можно меньше ломать текущую архитектуру:

- queue table остаётся общим источником сообщений
- subscription live table остаётся источником данных для consumer polling
- retry / fail / complete / history продолжают работать как сейчас

### Альтернативные решения

#### `lock by key`

Идея: использовать `pg_try_advisory_xact_lock(subscription, hash(key))` и синхронизировать обработку только на уровне `key`.

Простой `pg_try_advisory_xact_lock(subscription, hash(key))` хорошо решает только кейс "одно сообщение с данным key не должно
обрабатываться параллельно".

Но этого недостаточно для более сильной семантики:

- для одного `key` нужно обрабатывать не одно сообщение, а поток сообщений
- сообщения одного `key` должны идти строго по порядку
- желательно забирать несколько сообщений одного потока за один poll

Проблема в том, что порядок нужно обеспечивать уже на этапе `select`, а не только на этапе `handle`.
Если просто брать lock от `key` на отдельном сообщении, то мы не получаем удобной модели ordered batch processing.

Итог: `lock by key` недостаточен как основная модель для DBQ, если нужна именно ordered processing semantics внутри потока сообщений.

#### Линейный polling по всем partitions

Идея: после введения `logical_partition` consumer на каждом poll просто обходит `partition = 0..N-1` и пытается найти первую
partition с ready messages.

Если реализовать polling как "consumer идёт по `partition = 0..N-1` и проверяет, есть ли там ready messages", появляется
ещё одна серьёзная проблема, помимо самого lock mechanism.

Проблемы такого подхода:

- polling становится `O(number_of_partitions)` даже если ready messages мало
- при большом `maxPartitions` consumer тратит время на пустые partitions
- возникает bias в пользу "ранних" partition numbers, если всегда начинать обход с нуля
- hot partition может слишком часто выбираться первой
- при cluster concurrency несколько consumer-ов будут зря биться в один и тот же prefix partition numbers

То есть logical partitions нельзя эффективно poll-ить как фиксированный массив, который consumer каждый раз линейно сканирует.

Итог: линейный перебор partition ids слишком дорогой и создаёт проблемы fairness/latency.

### Вывод

Для DBQ нужен не `lock by key`, а routing сообщений в фиксированное число логических партиций.

То есть:

- у queue есть `maxPartitions`
- для каждого сообщения вычисляется `logicalPartition = hash(key) % maxPartitions`
- polling работает не "по key", а "по partition"

Это даёт:

- гарантированную последовательную обработку сообщений внутри одной partition
- возможность вычитывать batch сообщений одной partition в порядке
- контролируемую cluster-wide concurrency

### Упрощённая идея реализации

Полный coordinator с membership table, heartbeat, rebalance и отдельной таблицей ownership выглядит слишком тяжёлым для DBQ.

Более практичный вариант:

1. У queue задаётся фиксированное число logical partitions.
2. При вставке сообщения partition вычисляется из `key` и сохраняется в row.
3. Consumer при polling выбирает candidate partition, в которой есть ready messages.
4. Consumer пытается взять `pg_try_advisory_xact_lock(...)` на номер partition внутри текущей транзакции.
5. Если lock взят, consumer выбирает batch сообщений только из этой partition в нужном порядке.
6. Consumer обрабатывает batch последовательно в рамках одной poll iteration.
7. После commit/rollback advisory lock освобождается автоматически, и эту же partition может взять другой consumer.

### Что важно в этом варианте

- не нужна membership table
- не нужен heartbeat между consumer-ами
- не нужен долгоживущий ownership partition
- coordination живёт ровно столько, сколько живёт транзакция polling/processing
- архитектура DBQ почти не меняется: queue table остаётся source table, subscription table остаётся polling source

### Следствие для дизайна

Нужен polling не "по всем возможным partition ids", а "по partitions, в которых уже есть ready messages".

Из этого следует важное практическое решение:

- `logical_partition` лучше хранить не только в queue table, но и в subscription live table
- polling должен сначала находить небольшой набор candidate partitions из subscription table
- только после этого consumer должен пытаться брать advisory lock на candidate partition

Иначе каждый poll будет требовать либо перебор всех partitions, либо join в queue table уже на этапе поиска candidate partitions.

### Более практичный polling flow

1. В `subscription` хранится `logical_partition`.
2. Consumer выбирает не сообщения сразу, а несколько candidate partitions, где есть ready rows.
3. Candidate partitions упорядочиваются по "самому раннему ready message" в partition.
4. Consumer пытается взять advisory lock на одну из candidate partitions.
5. Если lock взят, consumer читает batch только из этой partition в порядке `execute_after`, затем `id`.

Это важно потому, что ordered processing делает partition единицей scheduling. Значит, сначала мы выбираем partition,
и только потом сообщения внутри неё.

### Практический вывод

Для `Logical Partitions` polling почти наверняка должен быть двухфазным:

1. `find candidate ready partitions`
2. `lock one partition and read ordered batch`

А не однофазным `select messages with lock`, как сейчас.

### Открытые вопросы

- где именно хранить `logical_partition`: по текущему анализу, почти наверняка и в queue table, и в subscription table
- каким должен быть порядок внутри partition: `execute_after`, затем `id`
- какой размер batch допустим, чтобы hot partition не монополизировала consumer thread
- как лучше подобрать количество partitions для конкретной queue
- сколько candidate partitions нужно брать за один poll, чтобы не упираться в already locked partition
- нужен ли локальный round-robin / randomization между candidate partitions, чтобы уменьшить contention
 

## Priority queue

Нужно добавить в DBQ механизм приоритетной очереди на базе текущей модели DBQ.
