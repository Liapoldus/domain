# Domain — целевой плагин v2

Платформенный milestone v2 требует саморегистрации через Plugin SDK:
уникальная identity/incarnation и lease каждой replica, SemVer/digest и
совместимость Raft log/FSM, ER schema и peer methods. Core не является
Raft-узлом и не управляет числом replicas. Две версии могут совместно
обрабатывать durable state только после conformance совместимости; иначе
rolling/canary блокируется. Gate и продуктовые задачи — в `TODO.md` владельца.

Domain — отдельный плагин-владелец общей ER-модели и данных. Core хранит
только точные байты желаемой JSON-конфигурации; плагин получает поколение через
Plugin SDK REST `Reload` и exact-generation pull. Core не знает сущностей,
миграций, SQL или состояния реплик.

## Конфигурация и миграции

Нормативная [схема модели](https://github.com/Liapoldus/domain/blob/main/contracts/v1/model.schema.json) находится
в этом репозитории. Сущность имеет одного владельца-группу; изменяемые поля
описываются типом и ограничениями. Сначала строится план миграции без записи
данных. Добавление nullable-поля или поля с default безопасно. Rename или
изменение типа требуют явного mapping; отсутствующее поле без mapping считается
потенциальным удалением и отклоняется. Само наличие mapping не отменяет проверку
всех существующих строк перед commit. Деструктивный drop не выводится автоматически.
В рамках одной сущности mapping обязан иметь уникальные пары источника и
назначения: повтор `from` или `to` отклоняется как `invalid_mapping`, чтобы
план не выбирал преобразование по порядку элементов и не сливал два поля в одно.
Runtime сейчас дополнительно проверяет допустимый синтаксис идентификаторов,
типы и структурные лимиты, а `references` должны указывать на существующее
primary/unique поле совпадающего типа. Эти проверки относятся к описанию модели;
внутренний SQLite row-write adapter также проверяет известные поля, scalar
типы, required/default и соответствие типизированного primary key storage ID:
text ID сохраняется напрямую, остальные scalar-типы кодируются type-prefixed
canonical value.
Уникальность и FK проверяются в пределах tenant+site; невалидная Raft-команда
не продвигает applied index. Локальная SQLite migration preflight проверяет
все преобразованные строки по candidate schema до изменения данных/модели и
отклоняет, например, миграцию, которая добавляет нарушенное unique-ограничение.
Это внутренние storage/FSM гарантии; публичный CRUD/batch появился отдельным
слоем — product peer API (см. «Продуктовый peer API») — и не отменяет их.
Ни те, ни этот API не заменяют кластерный Raft migration protocol.
Авторизация на peer API реализована: tenant/site scope берётся только из
mTLS identity, а ACL OwnerGroup проверяется до propose и внутри FSM.
`domain.migration.plan` и `domain.migration.status` — read-only методы. Методы,
которые самостоятельно применяют или откатывают ER-модель, не публикуются:
единственный источник желаемой модели — точная конфигурационная генерация Core,
полученная плагином при SDK `Reload`/pull. Migration preflight, Raft commit и
snapshot restore остаются внутренней application-механикой конфигурационного
applier; production SDK composition и сквозной ACK ещё не подключены (см.
`TODO.md`).

При обработке Core generation Domain применяет миграцию как одну согласованную
операцию Raft. До её завершения несовместимые Runtime-группы fenced. Ошибка до
commit оставляет прежнюю модель. Core rollback выражается новым активным
поколением; внутренний applier восстанавливает снимок до миграции: записи после
снимка удаляются из активного состояния, фиксируются audit и новый data epoch.
Это намеренная потеря данных; оператор обязан учитывать её при откате. Полная
связка SDK Reload/pull → commit → generation ACK пока не реализована.

## Хранение и согласованность

SQL SQLite-адаптера принадлежит Go-коду:
`internal/infrastructure/sqlite/queries.go` содержит именованные запросы,
включая schema, migration/rollback, snapshot, ledger/audit и Raft storage.
Внешних `.sql` assets и runtime loader нет. Перенос сохраняет точные байты
и позиционную привязку параметров; native Go tests рядом с адаптером фиксируют
SHA-256, длины, placeholders и проверяют scoped CRUD и BLOB-параметры Raft.

Минимум три голосующих узла Domain. Raft log — долговременный упорядоченный
источник команд; каждый узел детерминированно применяет их к локальной SQLite
и строит immutable in-memory snapshot. Запись успешна после majority commit.
При утрате quorum записи и свежие чтения недоступны. Каждое чтение берёт
barrier у текущего лидера и ждёт соответствующий applied index реплики; stale
данные не возвращаются. Core не участвует в выборах лидера. Продуктовый Raft
transport использует generic peer API `pluginprotocol`.

Snapshot FSM версионирован: поле `formatVersion` (текущий формат — 2,
отсутствие/0 читается как legacy v1) и ledger идемпотентности `write_ledger`;
старые snapshots импортируются с пустым ledger, а prune ledger на restore
детерминированно повторяет правило cap. Ledger пишется той же
SQLite-транзакцией, что и состояние: `seq` — raft log index первого
применения команды, глобальный cap 4096 записей, за его пределами
вытесняются записи с наименьшим `seq`. Повторное применение того же
`writeId` не меняет состояние и отвечает `duplicate:true`. Атомарность batch —
одна SQLite-транзакция на всю команду: частичного состояния не бывает, ошибка
любой операции отклоняет весь batch и не продвигает applied index. Удаление
строки идёт в режиме FK-restrict: ссылающиеся строки внутри tenant+site
блокируют delete (`conflict`).

## Продуктовый peer API

Семь методов — `domain.create`, `domain.update`, `domain.delete`,
`domain.batch`, `domain.get`, `domain.query`, `domain.cluster.status` —
зарегистрированы на том же mTLS endpoint, что и Raft: расширение
`internal/infrastructure/raftpeer` держит один listener, один registry и
method-prefix authorizer. Методы `domain.*` допускают только identities из
конфигурационного списка `ProductCallers` (config-driven, как
`PeerIdentities`); raft-методы сохраняют прежнюю политику identity без
изменений. Если список product calls пуст, поведение endpoint'а идентично
прежнему raft-only варианту.

Контракт вызова — JSON envelope. Успех: `{"ok":true,"data":...}`; бизнес-ошибка
не становится protocol-level ошибкой транспорта, она идёт внутри payload:
`{"ok":false,"error":{"code","retryable","unknownOutcome","message"}}`.
Коды: `invalid_request`, `forbidden`, `not_found`, `conflict`, `not_leader`,
`unavailable`, `unknown_outcome`, `query_rejected`, `internal`
(`contracts/v1/schemas/envelope.json`). Тексты ошибок не содержат строк,
значений SQL-параметров, credentials или секретов tenant.

Отдельные правила:
- `not_leader`: записи и batch на follower'е отклоняются с hint
  `leaderAddress`; `domain.get` и `domain.query` один раз форвардятся текущему
  лидеру. Повторного proxying нет; недоступный target даёт `unavailable`, а
  истёкший deadline — `unknown_outcome`.
- `unavailable`: потеря quorum, отказ barrier или transport; свежее чтение и
  запись недоступны, stale данные не отдаются никогда.
- `unknown_outcome`: apply timeout/дедлайн контекста; повтор с тем же `writeId`
  применяется ровно один раз.
- `forbidden`: ACL OwnerGroup (group вызывающего ≠ owner group сущности),
  проверяется до propose и повторно внутри FSM.
- `conflict`/`not_found`: ER-нарушения (unique, FK-restrict, отсутствующая или
  уже существующая строка) из FSM-сентинелов.
- `query_rejected`: грамматика, валидация или лимиты SELECT.

Scope и ACL: запрос не может задать tenant/site/ownerGroup — этих полей нет ни
в одной request-схеме, а scope разрешается из аутентифицированного URI SAN
mTLS-соединения через конфигурационный маппинг
`ProductCallers {Identity, Tenant, Site, Group}`. Неизвестная identity для
`domain.*` отклоняется authorizer'ом до handler'а. Tenants и sites разделяются
разными identities в конфигурации; payload со scope-полями отклоняется как
`invalid_request`.

Идемпотентность: `writeId` (`^[A-Za-z0-9_-]{1,64}$`) опционален; он попадает в
ledger `write_ledger` той же транзакцией, что и изменение состояния, и повтор
того же `writeId` в scope tenant+site отвечает успехом с `duplicate:true` без
изменения данных. `domain.batch` принимает 1..100 операций
create/update/delete и применяется атомарно. Per-capability лимиты и таймауты
объявлены в `contracts/v1/plugin.json`: payload 1 MiB (batch 2 MiB);
create/update/delete 15000 ms, batch 30000 ms, get/query 10000 ms,
status 3000 ms.

Свежее чтение: `domain.get` и `domain.query` выполняются только на лидере
после barrier (`CommitReadBarrier` и ожидание applied index на выбранной
replica); write-путь также проходит leader check и barrier.
`domain.cluster.status` доступен с любого узла и отдаёт
`ready, leader, leaderAddress, term, commitIndex, appliedIndex, voters,
quorum, epoch` — это метод наблюдаемости, не readiness/lifecycle endpoint
плагина. Каждый ответ несёт `appliedIndex` и `epoch`.

Request/response схемы — `contracts/v1/schemas/*.json` (JSON Schema 2020-12, по
одной на метод) — это контракт. Dispatch до проверки схемы отклоняет повторные
ключи JSON-объектов на любой глубине; сравниваются уже декодированные имена,
поэтому буквальная и `\u`-запись одного ключа тоже считается повтором. Это
исключает разное толкование одного запроса валидатором, декодером и FSM. Затем
payload проверяется скомпилированной при запуске runtime JSON Schema; ошибки
содержат только JSON Pointer и имя нарушенного keyword, без значений запроса.
Поведение покрыто `tests/schema-validation.test.ts`,
`tests/product-api.test.ts`, `tests/product-commands.test.ts` и трёхузловым mTLS
e2e `tests/product-peer.test.ts`.

## Доступ и запросы

CRUD и batch — product-owned peer methods (см. «Продуктовый peer API»). Один
batch атомарен внутри Data Model; транзакция между плагинами не обещается.
Каждый вызов имеет доверенный tenant/site scope и authenticated group
identity, разрешённые из mTLS identity. Плагин проверяет доступ к сущности по
OwnerGroup, а не доверяет scope, произвольно записанному в payload: таких полей
в request-схемах нет, и попытка их передать отклоняется.

Аналитика принимает только параметризованный SELECT одним оператором:
`SELECT [DISTINCT] <cols|agg|*> FROM <entity>[ alias]
([INNER|LEFT] JOIN <entity>[ alias] ON <col> = <col>)*
[WHERE <bool>] [GROUP BY <col>,...] [HAVING <bool>]
[ORDER BY <col> [ASC|DESC],...] [LIMIT n [OFFSET m]]`.
JOIN разрешён только по объявленным `references`, выражения — `= != <> < <= >
>=`, `AND`/`OR`/`NOT`, скобки, литералы и параметры `?`; агрегаты —
COUNT/SUM/MIN/MAX/AVG. SQL-записи, подзапросы, UNION, оконные функции и
неограниченные результаты не поддерживаются. Лимиты statement'а: текст ≤8192 B,
параметры ≤64, LIMIT/OFFSET ≤10000 (по умолчанию 100), сканируется ≤200000
строк; нарушение — `query_rejected`. Поле `maxRows` запроса может только
уменьшить результат (значение >10000 отклоняется presentation-слоем как
`invalid_request`, как и >64 параметров). Без ORDER BY результат сортируется
лексикографически по полной выходной строке, поэтому порядок детерминирован.
Чтение идёт после barrier на лидере в пределах tenant/site.

Реализация планировщика —
[`internal/application/query*.go`](https://github.com/Liapoldus/domain/blob/main/internal/application/query.go)
(lexer, parser, валидация, executor). GLinq
(`github.com/CreateLab/glinq`) поставляет streaming-операторы
Where/GroupBy/OrderBy/DistinctBy/Skip/Take и используется там, где он подходит;
парсер, проход валидации, nested-loop equijoin (в glinq нет join-оператора),
свёртки агрегатов по динамическим значениям и полный порядок сравнения
значений написаны вручную. Итоговая сортировка всегда тотальна, поэтому
map-порядок GroupBy в glinq не влияет на результат. Покрытие —
`tests/query-planner.test.ts`.

## Совместимость контракта

`contracts/v1/plugin.json` объявляет блок `compatibility`:
`raftLogCommandKinds` — `init|put|migrate|rollback|create|update|delete|batch`;
`fsmSnapshotFormatVersion` — 2; `erSchemaVersion` — `"1"`; `peerContracts` —
`liapoldus.domain.raft-peer.v1` и `liapoldus.domain.product-peer.v1`. Каждая
capability несёт относительные пути `payloadSchema`, `responseSchema`,
`errorSchema` плюс `maxPayloadBytes` и `timeoutMs`; схемы лежат в
`contracts/v1/schemas/*.json` и вшиты в Go-бандл через `contracts/assets.go`.
Соответствие блока kinds фактическим командам FSM, enum кодов ошибок envelope
и запрет tenant/site/ownerGroup во всех request-схемах проверяется
`tests/contracts.test.ts`. Публикация SemVer/digest и объявление совместимости
на уровне релиза остаются открытыми: `compatibility` — версия собственного
контракта плагина, не готовность релиза.

## Статус реализации

Поставлен product peer API: 7 методов на общем с Raft mTLS endpoint,
JSON envelope с кодами ошибок, tenant/site scope только из mTLS identity, ACL
OwnerGroup до propose и в FSM, идемпотентность по `writeId`, snapshot format v2
с ledger и SELECT-планировщик (подробности — в разделах выше). Suite на
2026-10-07: 16 файлов / 104 теста Vitest; `go build ./...`, `go vet ./...`,
`go test ./...` и `git diff --check` чисто. Ниже — состояние storage,
transport и fixture-слоёв.

Сейчас реализованы план, локальное транзакционное применение миграции к SQLite,
снимок до миграции и локальный rollback с потерей последующих записей. Проверены
reopen и откат транзакции при ошибке. Durable SQLite `LogStore`/`StableStore`
проверены трёхузловым кластером с полным перезапуском; fixture также проверяет
commit после failover и отказ записи без большинства. Domain-owned adapter
реализует HashiCorp `raft.Transport` поверх generic peer API `pluginprotocol`:
vote/pre-vote, AppendEntries и `TimeoutNow` используют unary calls; heartbeat
идёт по fast path; snapshot передаётся ограниченными bidi-stream frames без
буферизации целого снимка. mTLS обязателен, adapter сверяет URI SAN, Raft
ServerID и объявленный адрес узла. Child-process conformance проверяет RPC,
сохранность байтов snapshot и отказы при чужой identity, неверном server
identity и spoofed ServerID. Отдельный TypeScript child-process test запускает
трёхузловой HashiCorp Raft cluster через эти TCP/mTLS adapters и SQLite
LogStore/StableStore/FSM: проверяет election, majority commit, отказ записи
после остановки двух узлов, запуск двух оставшихся из durable stores, нового
лидера и commit после failover. Это подтверждает quorum/failover путь при
отказе процесса. Fixture также проверяет, что запись без quorum возвращает
ошибку и не появляется в локальной FSM; после перезапуска peer endpoint с теми
же адресом и mTLS identity новые majority commits успешны, а реплика догоняет
их. Majority commit при остановке одного из трёх узлов тоже проверен. Отдельный
`tests/raft-peer-network-partition.test.ts` собирает Linux child fixtures в
приватном network namespace. Двухпроцессный сценарий проверяет реальный TCP
reconnect, а трёхпроцессный запускает отдельные Raft nodes с разными OS UID;
`iptables` на уровне ядра отбрасывает входящие и исходящие пакеты изолированного
peer endpoint. Счётчики подтверждают drop. Два связанных узла продолжают
majority commits, изолированная живая replica остаётся stale и отклоняет write
и fresh-read fence; после снятия правила тот же PID/incarnation догоняет state,
и кластер снова фиксирует запись большинством. На macOS fixtures исполняются в
OrbStack Linux VM без постоянного изменения сетевых настроек хоста. Обычный
unprivileged Linux пропускает только OS-level сценарии, когда probe подтверждает,
что `unshare --net` запрещён; unit-проверки обнаружения runner остаются активны.
Обязательный Linux CI gate задаёт `DOMAIN_REQUIRE_OS_PARTITION=1`, поэтому
отсутствие требуемой capability завершает тест ошибкой. Локальный запуск пары
OS-partition тестов с этим флагом 2026-10-07 прошёл 4/4 на macOS; hosted
`ubuntu-24.04` run не выполнялся, поэтому сам fixture gate не закрывает hosted
CI gate и production lifecycle — они остаются открытыми в
[TODO](https://github.com/Liapoldus/domain/blob/main/TODO.md). Публичный
read/write API и tenant ACL закрыты отдельной поставкой product peer API
(см. выше).
Дополнительный fault-injection test перехватывает Raft RPC transport и
изолирует одну работающую реплику: она не становится leader и не применяет
majority commit, а после восстановления связи догоняет committed state. Это
in-process проверка raft peer отказа, не замена полной сетевой изоляции кластера.
Append pipeline не поддерживается — HashiCorp Raft
переходит на обычный AppendEntries.
FSM атомарно сохраняет applied index вместе с каждой командой, а повторное
применение того же Raft log index становится no-op. Snapshot несёт этот index;
восстановление модели, строк и указателя выполняется одной SQLite-транзакцией.
Child-process conformance проверяет replay уже применённой команды и сохранение
applied index после snapshot restore. Внутренний fresh-read gate теперь
подтверждает leadership/quorum на лидере, берёт durable index materialized FSM
и ждёт достижения этого индекса выбранной SQLite replica (нулевой индекс
разрешён только для подтверждённого пустого состояния); при partition, потере
quorum или deadline он не разрешает stale read. Трёхузловой TS fixture проверяет
отказ от чтения на отставшей replica, разрешение после сходимости и чтение
пустого состояния при доступном quorum.
Этот primitive подключён к публичному product read/write dispatch
(`domain.get`, `domain.query` и write-путь идут через barrier на лидере),
клиентская identity/ACL реализованы через `ProductCallers` и OwnerGroup;
подтверждённый hosted Linux CI runner остаётся открытой задачей. Snapshot FSM
ограничен 512 MiB по умолчанию: строки сериализуются и восстанавливаются потоково через временный
файл, а SQLite restore выполняется атомарно. TypeScript conformance проверяет
компактизацию Raft log, аварийное завершение процесса и восстановление состояния
после перезапуска, а также отклонение snapshot сверх лимита без потери активных
данных. Product peer API покрыт `tests/product-api.test.ts`,
`tests/product-commands.test.ts` и трёхузловым mTLS e2e
`tests/product-peer.test.ts` (включая восстановление после потери quorum).
Открытыми остаются hosted `ubuntu-24.04` CI gate, Core-side integration,
SDK lifecycle/саморегистрация (composition в `cmd/`, readiness beyond
`domain.cluster.status`, Reload, generation ACK, CLI), production migration
applier через SDK Reload/pull и объявление SemVer/digest — полный
список в [TODO](https://github.com/Liapoldus/domain/blob/main/TODO.md). Этот
документ описывает достигнутое и целевое v2-поведение и не объявляет плагин
production-ready. Все незавершённые пункты относятся к общему v2;
`contracts/v1` — версия собственного контракта плагина, не обещание релиза Liapoldus v1.
