# Domain — задачи общего v2

Срез source-owned SQL — 2026-10-09: все 40 SQL assets из
`internal/infrastructure/sqlite/sql/` перенесены в именованные Go-константы
`internal/infrastructure/sqlite/queries.go`. SHA-256, длины и placeholders
зафиксированы до переноса по текущему dirty WIP (включая `schema.sql` и
`import_reset.sql`) в `queries_test.go`; там же перечислены точные имена всех
40 удалённых assets (20 tracked и 20 untracked). Удалены только перенесённые
файлы, SQLite `statements embed.FS`, `//go:embed sql/*.sql`, `statement()` и
его imports `embed`, `io/fs`, `fmt`. Все 40 queries имеют production consumers;
аудит 59 SQLite call expressions подтвердил сохранение аргументов и их порядка.
Migration/rollback, audit/ledger, schema и snapshot history/format сохранены;
production JSON/SELECT parsers уже принадлежат Go-коду и не менялись.
Native Go tests также проверяют scoped CRUD с SQL-подобными bound values,
повторные Raft BLOB parameters, nil/empty blobs и inclusive log deletion.

Проверки с `GOWORK=off`: `go test ./internal/infrastructure/sqlite -count=1`
(3 теста, включая 40 byte-preservation subtests), `go build ./...`,
`go vet ./...`, `go test ./... -count=1` — PASS. Фокусный Vitest
(`sqlite-migration`, `cluster-migration`, `raft-fsm`, `raft-durable-storage`,
`raft-bounded-snapshot`, `product-commands`, `row-constraints`,
`--maxWorkers=1`) — 7 файлов / 21 тест PASS. Полный
`GOWORK=off npm test -- --maxWorkers=1` — 21 файл / 144 теста PASS,
без skipped; `git diff --check` — PASS. Go unit tests разрешены пользователем,
owner `AGENTS.md` и storage docs обновлены только для SQL/test среза.
Production SDK composition/lifecycle, Core integration и hosted Linux required
check остаются открытыми ниже. Другие репозитории, commits и push не затронуты.

Срез миграционной безопасности — 2026-10-08: планировщик отклоняет
неоднозначные mapping с повторным `from` или `to` внутри одной сущности как
`invalid_mapping`; покрыто child-process сценарием
`tests/migration.test.ts` (9/9). Полный Vitest запущен: 20/21 файлов и
143/144 теста прошли; существующий Linux OS-partition тест завершился пустым
выводом дочернего процесса (`Unexpected end of JSON input`). Этот сбой не
связан с миграционным срезом и требует отдельного расследования. Для текущего
дерева `GOWORK=off go build ./...`, `GOWORK=off go vet ./...` и
`git diff --check` прошли.

Срез проверки product JSON — 2026-10-08: до runtime JSON Schema dispatch
отклоняет повторяющиеся ключи исходного JSON на любой глубине, включая
экранированные варианты одного имени. Child-process test подтверждает, что
неоднозначный `domain.create` получает `invalid_request` и не изменяет FSM.
Полный локальный Vitest: 21 файл / 143 теста; `GOWORK=off go build ./...`,
`GOWORK=off go vet ./...` и `git diff --check` прошли на macOS. Hosted Linux,
Plugin SDK lifecycle и production composition этим срезом не закрываются.

Повторная Linux/OrbStack-проверка — 2026-10-08: в работающей OrbStack VM
`docker info` подтвердил `linux/aarch64`. Команда
`DOMAIN_REQUIRE_OS_PARTITION=1 npx vitest run tests/linux-partition-ci.test.ts
tests/raft-peer-network-partition.test.ts --maxWorkers=1` прошла 2 файла / 4
теста. Настоящие Linux child binaries выполнялись в приватных network
namespaces; kernel packet-drop, сохранение живых процессов, отказ изолированной
реплики и catch-up после восстановления подтвердились. Это закрывает локальный
host-level Linux VM runtime gate, но не hosted `ubuntu-24.04` CI result,
branch-protection required status check, Core integration или SDK lifecycle.

Повторная локальная проверка WIP v2 (runtime JSON Schema dispatch, epoch
fencing, cluster migration/rollback API, follower read proxying) — 2026-10-07:
полный Vitest suite прошёл (21 файл / 136 тестов), затем `go build ./...`,
`go vet ./...`, `go test ./...` и `git diff --check` прошли. Это локальная
macOS проверка; hosted `ubuntu-24.04` job, Core-side integration и SDK
lifecycle остаются открытыми ниже.

Повторная локальная проверка поставки product peer API — 2026-10-07: полный
Vitest suite прошёл (16 файлов / 104 теста), затем `go build ./...`,
`go vet ./...`, `go test ./...` и `git diff --check` прошли. Локальный
OS-partition запуск `DOMAIN_REQUIRE_OS_PARTITION=1 npx vitest run
tests/raft-peer-network-partition.test.ts tests/linux-partition-ci.test.ts`
прошёл 4/4 на darwin. Это локальная macOS проверка; hosted `ubuntu-24.04`
job, Core-side integration, SDK lifecycle и required branch protection status
остаются открытыми ниже.

Повторная локальная проверка текущего WIP — 2026-10-06: полный Vitest suite
прошёл (12 файлов / 25 тестов), затем `go test ./...`, `go build ./...`,
`go vet ./...` и `git diff --check` прошли. Это локальная macOS проверка;
обязательный hosted `ubuntu-24.04` OS-partition job и required branch
protection status остаются открытыми ниже.

Начальные локальные срезы сохранены как прототип. Все незакрытые пункты ниже
относятся к общему релизу Liapoldus v2 и не входят в приёмку v1. Версия
`contracts/v1` обозначает первую версию контракта самого плагина, а не этап
экосистемы.

Milestone платформы перед production integration: перейти на Plugin SDK
self-registration/lease, объявлять SemVer/digest и совместимость Raft log/FSM,
ER schema и peer contracts. Две когорты Domain нельзя одновременно допускать
к одному Raft cluster или SQLite state без доказанной совместимости. Core
наблюдает replicas и назначает config generation, но не выбирает лидера,
не масштабирует узлы и не участвует в quorum. Подробный rollout contract — в
[Core deployment design](https://liapoldus.github.io/core/architecture/plugin-deployment).

- [ ] Gate платформы: замена узла новой incarnation, lease expiry, mixed-version
  Raft compatibility, отклонение несовместимого canary и восстановление после
  рестарта Core без записи payload в Core.

## P0 — блокеры интеграции и единого источника конфигурации

- [x] Удалить независимую запись ER-модели из product peer API: в manifest и
  runtime dispatch оставлены только read-only `domain.migration.plan` и
  `domain.migration.status`; `domain.migration.apply` и
  `domain.migration.rollback` больше не зарегистрированы. Проверяется
  `tests/contracts.test.ts` и неизвестный метод в cluster fixture. Внутренние
  `MigrationService.Apply/Rollback` сохранены как application machinery, не как
  пользовательские методы.
- [ ] Подключить сохранённую migration machinery к Plugin SDK ConfigApplier:
  только точные raw bytes generation, полученные при `Reload`/pull, могут
  изменить модель; ACK отправляется лишь после кластерного применения. Core
  rollback должен прийти как новое активное поколение и запустить согласованное
  восстановление snapshot. Сейчас production SDK composition и сквозной
  Core config → Reload/pull → migration/ACK → rollback path отсутствуют.
- [ ] Добавить production composition/bootstrap для трёх Raft nodes. Сейчас
  отсутствуют `cmd/` entrypoint и контракт начального формирования voter set;
  `raftpeer.Config` требует ручного соответствия endpoint → URI SAN и endpoint
  → Raft ServerID, а трёхузловой bootstrap существует только в fixtures.
  Зафиксировать разделение локальных operator bootstrap values (Core URL,
  local instance/replica identity, REST/peer bind, PEM/CRL paths) и Core-owned
  desired voter membership; определить безопасный initial quorum, restart,
  add/remove и replacement transitions без auto-bootstrap поверх существующей
  SQLite. Gate — три отдельно запускаемых production binaries проходят
  registration/Reload, election, restart и membership transition без fixture
  composition.
- [ ] Не подключать локальные SDK APIs через `replace` или workspace-only
  dependency. Published Plugin SDK `v1.0.1` не содержит registration/lease и
  peer-directory contracts; использовать только после публикации совместимой
  immutable SDK revision и её точного pin в Domain/Core.

- [x] Версионированная начальная ER-схема и тестируемый план безопасных миграций.
- [x] Начальный product Manifest с собственными методами; payload/response/error
  schemas вынесены в `contracts/v1/schemas/*.json`, связаны из
  `contracts/v1/plugin.json` и проверены `tests/contracts.test.ts`. Регистрация
  7 методов на общем mTLS endpoint сделана в `internal/infrastructure/raftpeer`
  (`ProductCalls`/`ProductCallers`). Самостоятельная runtime registration через
  Plugin SDK self-registration/lease остаётся открытой (см. milestone выше).
- [x] Начальная runtime-проверка model schema: идентификаторы, enum типов,
  лимиты entities/fields/mappings, scalar defaults, duplicate names и ссылки на
  существующее PK/unique поле совпадающего типа. Red→green покрыто
  `tests/migration.test.ts`.
- [x] Внутренний SQLite row-write adapter валидирует JSON object, известные
  поля, scalar types, required/default и согласованность typed primary key с
  storage ID (`text` сохраняется напрямую, прочие типы используют
  type-prefixed canonical value); уникальность и FK проверяются в пределах
  tenant+site. Ошибка отклоняет FSM-команду без продвижения `last_applied_index`.
  Red→green: `tests/row-constraints.test.ts`. Это внутренний adapter, а не
  публичный CRUD/API (публичный peer API появился отдельным слоем, см. пункт про
  Product-owned peer CRUD ниже) и не полная JSON Schema validation.
- [x] Локальная SQLite migration preflight преобразует все строки, затем до
  обновления данных и модели проверяет их по candidate schema, включая новые
  unique constraints; конфликт оставляет предыдущую модель и все строки
  неизменными. Red→green: сценарий duplicate values в
  `tests/sqlite-migration.test.ts`. Это локальная транзакционная гарантия, не
  кластерный Raft migration protocol.
- [x] Публичный dispatch берёт tenant/site только из mTLS identity
  (`ProductCallers` в `internal/infrastructure/raftpeer`), отвергает эти поля
  в payload как `invalid_request` и проверяет OwnerGroup до propose и внутри
  FSM. Покрытие: `tests/contracts.test.ts`, `tests/product-api.test.ts`,
  `tests/product-peer.test.ts`.
- [x] Заменить ручную строгую декодировку запросов в
  `internal/presentation/product/requests.go` полной runtime JSON Schema
  validation. Dispatch компилирует embedded contract schemas draft 2020-12
  один раз в `NewHandler`, затем валидирует входящие payload по request schema,
  назначенной каждому зарегистрированному методу в
  `internal/presentation/product/schema.go`; ошибки — `invalid_request` с JSON
  pointer + keyword и без значений payload. Red→green:
  `tests/schema-validation.test.ts`. До schema validation исходный token stream
  также проверяется на повторные object keys (включая эквивалентные после
  unescape имена); child-process подтверждает отказ до записи в FSM.
- [x] Реализовать согласованную кластерную migration/rollback с fencing:
  crash recovery между Raft snapshots/log и audit в той же FSM-транзакции, что
  и смена модели. TypeScript cluster fixture вызывает application lifecycle
  напрямую как тестовый adapter; публичные `apply`/`rollback` peer methods
  отсутствуют. Покрытие: `tests/cluster-migration.test.ts`,
  `tests/contracts.test.ts`.
- [x] Локальное атомарное применение migration plan к SQLite с preflight, rollback транзакции при ошибке и проверкой после reopen.
- [x] Локальный SQLite pre-migration snapshot и rollback с удалением post-migration записей из активного состояния.
- [x] Кластерная семантика migration/rollback: crash recovery между snapshots
  и Raft log, fencing data epoch для клиентских reads/writes и audit. Internal
  application lifecycle подтверждён fixture tests; публично доступны только
  read-only plan/status. Покрытие: `tests/cluster-migration.test.ts`,
  `tests/epoch-fencing.test.ts`.
- [x] Тестовая трёхузловая Raft-сборка на in-memory transport: leader election, majority commit, failover и отказ записи без quorum.
- [x] SQLite FSM apply/snapshot/restore через Hashicorp Raft interfaces; подтверждено локальным тестом.
- [x] Durable FSM applied-index fence: запись команды/миграции/rollback и
  `last_applied_index` фиксируются одной SQLite-транзакцией; повторный log index
  отвечает no-op, ошибка команды не двигает указатель. Raft barrier и
  configuration entries также durable-advance указатель.
- [x] Durable SQLite Raft `LogStore`/`StableStore` реализованы на HashiCorp
  interfaces. TypeScript integration fixture запускает трёхузловой кластер на
  этих stores, останавливает все Raft nodes и SQLite handles, открывает их
  заново и подтверждает сохранность стабильных метаданных, committed logs и
  materialized rows. Тот же fixture проверяет новый leader после изоляции
  прежнего и отказ записи без quorum. Это закрывает только durable local
  stores и in-process fault injection; сетевой peer transport описан отдельным
  пунктом ниже.
- [x] Domain-owned Raft peer contract и adapter `raft.Transport` поверх generic
  `pluginprotocol`: unary vote/pre-vote/append/timeout-now, heartbeat fast path,
  bounded streaming snapshot и обязательный per-node mTLS с привязкой URI SAN,
  Raft ServerID и объявленного peer address. Child-process conformance проверяет
  heartbeat, append, snapshot bytes, pin mismatch, неизвестную identity и spoofed
  ServerID. Append pipeline намеренно не поддержан; HashiCorp Raft переходит на
  обычный AppendEntries.
- [x] Трёхузловой Raft cluster поверх production adapter: TypeScript запускает
  реальный HashiCorp Raft fixture с тремя TCP/mTLS `raftpeer.Transport`, SQLite
  LogStore/StableStore и SQLite FSM. Проверены election, majority commit, отказ
  записи у оставшегося узла без quorum, остановка прежнего лидера, повторный
  запуск двух узлов из SQLite и новый majority commit после election.
- [x] Conformance отказа и восстановления peer endpoint на real adapter:
  после потери quorum Raft write возвращает ошибку и не меняет локальную FSM;
  после перезапуска peer с теми же адресом и mTLS identity кластер принимает
  новые majority commits, а вернувшаяся реплика догоняет применённый ряд.
  Дополнительно проверен majority commit, пока один из трёх узлов остановлен.
- [x] Fault-injection Raft transport partition без остановки follower process:
  изолированный follower не становится leader и не применяет majority commit,
  пока две связанные реплики продолжают работу; после восстановления RPC-трафика
  follower догоняет committed state. Это тестовая обёртка над transport API,
  а не проверка потери TCP-пакетов или сетевой политики ОС.
- [x] Фокусный TCP/OS partition/reconnect gate для двух отдельных child processes:
  TypeScript собирает Linux fixture и запускает её в изолированном loopback-only
  network namespace; kernel `iptables` drop rule проверяется по packet counter.
  Оба процесса остаются живы, вызов через реальный mTLS peer transport
  завершается ошибкой, затем тот же client process создаёт соединение заново и
  успешно вызывает peer после удаления правила. На macOS gate использует
  OrbStack Linux VM; правила существуют только в дочернем network namespace.
- [x] Фокусный трёхузловой TCP/OS partition/reconnect gate без остановки процессов:
  `tests/raft-peer-network-partition.test.ts` запускает три независимых Raft
  child process с разными OS UID в приватном Linux network namespace и применяет
  kernel `iptables` drop к TCP endpoint одной реплики. Gate проверяет majority
  commit двух связанных узлов, stale state и отказ fenced read/write на изолированной
  реплике, фактический drop пакетов, сохранение PID/incarnation и catch-up после
  снятия правила, а затем новый majority commit. На macOS Linux fixture запускается
  в OrbStack; постоянные сетевые настройки хоста не меняются. Это fixture gate:
  сам по себе он не закрывает публичный Domain API и tenant ACL (это закрыто
  отдельной поставкой product peer API ниже), production lifecycle и hosted
  CI gate остаются открытыми.
- [ ] Получить hosted-result отдельного `.github/workflows/verify.yml` на
  `ubuntu-24.04`. Workflow устанавливает `DOMAIN_REQUIRE_OS_PARTITION=1` и
  запускает privileged OS partition suite через `sudo -E`: отсутствие
  `unshare --net` capability должно завершать job ошибкой, а не skip. После
  зелёного hosted run настроить branch protection так, чтобы job
  `Linux Raft OS partition (required capability)` был required status check.
  Локальный OrbStack PASS и наличие workflow сами по себе этого gate не
  закрывают. Локальный запуск с `DOMAIN_REQUIRE_OS_PARTITION=1` 2026-10-07 на
  darwin прошёл 4/4 (`tests/raft-peer-network-partition.test.ts`,
  `tests/linux-partition-ci.test.ts`); hosted run не выполнялся.
- [x] Внутренний fresh-read barrier primitive: лидер должен подтвердить
  leadership/quorum, выполнить Raft barrier и выдать durable FSM applied index;
  выбранная SQLite replica ждёт применения этого индекса и отвечает unavailable
  по deadline вместо stale read. TS child-process conformance проверяет
  fencing отстающей replica, допуск после сходимости, отказ без quorum и
  допустимый индекс 0 для пустого состояния.
- [x] Подключить fresh-read gate к публичному Domain read path и peer/API
  dispatch. `domain.get`, `domain.query` и write-путь идут через barrier
  (`internal/application/product_service.go` → `Cluster.Barrier` в
  `internal/infrastructure/raftfsm/cluster.go`: `CommitReadBarrier` +
  `WaitForAppliedIndex`); отказ barrier → `unavailable`, stale данные не
  отдаются. Ошибки, deadline и лимиты проходят через versioned product contract
  (`contracts/v1/schemas/envelope.json`, per-method схемы, `timeoutMs` в
  `contracts/v1/plugin.json`). Покрытие: `tests/product-api.test.ts`,
  `tests/product-peer.test.ts`.
- [x] Прозрачное перенаправление read-запросов follower'а на лидера. Сейчас
  follower отвечает `not_leader` с `leaderAddress` и клиент повторяет вызов сам;
  proxying не реализован и заявлен open item. `not_leader`-поведение покрыто
  `tests/product-api.test.ts` и `tests/product-peer.test.ts`.
  Реализация: follower прозрачно форвардит `domain.get`/`domain.query` на
  лидера с зарезервированным claim `forwardedScope`; покрытие —
  `tests/follower-proxy.test.ts`, `tests/schema-validation.test.ts`,
  `tests/product-peer.test.ts`.
- [ ] Production lifecycle/composition и readiness API для Raft-кластера.
  Domain peer transport контракт определяет только RPC transport, не readiness
  или leadership API. Fixture наблюдает HashiCorp Raft leader state и commit;
  это не публичный readiness endpoint. Метод `domain.cluster.status` (с любого
  узла отдаёт ready/leader/term/commitIndex/appliedIndex/voters/quorum/epoch)
  не закрывает этот пункт: composition в `cmd/` отсутствует, SDK
  registration/lease не подключены — Plugin SDK v2 contract/implementation пока
  существует только в локальном незавершённом workspace, не закреплён совместимой
  опубликованной ревизией.
- [x] Bounded streaming Raft snapshots: FSM последовательно сериализует строки
  и durable applied index в ограниченный временный файл (по умолчанию ≤512
  MiB); restore staging-ит поток на диск и атомарно применяет модель, строки и
  applied index в SQLite. TypeScript conformance принудительно создаёт/
  компактирует snapshot, аварийно завершает процесс, очищает materialized state
  и проверяет восстановление данных и applied index после перезапуска; превышение
  лимита при создании и restore отклоняется без изменения active state.
- [x] Публичный fresh-read dispatch, end-to-end fencing, идемпотентность и
  unknown-outcome writes. Продуктовый dispatch `internal/presentation/product`
  поверх `ProductService`: чтения и записи идут на лидере после barrier, ledger
  `write_ledger` даёт идемпотентность по `writeId` (seq = raft log index первого
  применения, cap 4096 с детерминированным prune,
  `internal/infrastructure/sqlite/ledger.go`), replay отвечает
  `duplicate:true`; apply timeout/ctx deadline → `unknown_outcome`
  (`unknownOutcome:true`), повтор с тем же `writeId` применяется ровно один
  раз. Покрытие: `tests/product-api.test.ts`, `tests/product-commands.test.ts`,
  `tests/product-peer.test.ts` (трёхузловой mTLS e2e, включая восстановление
  после потери quorum).
- [x] Product-owned peer CRUD, ACID batch, tenant/site ACL, ER constraints и
  проверка identity caller. Семь методов
  `domain.create/update/delete/batch/get/query/cluster.status` зарегистрированы
  на том же mTLS endpoint, что и Raft (`internal/infrastructure/raftpeer`,
  method-prefix authorizer); scope только из mTLS URI SAN (`ProductCallers`),
  payload-поля tenant/site/ownerGroup → `invalid_request`; batch — одна SQLite
  транзакция без частичного состояния, FK-restrict delete, ACL OwnerGroup до
  propose и в FSM (`internal/infrastructure/sqlite/product_commands.go`).
  Покрытие: `tests/product-commands.test.ts`, `tests/product-api.test.ts`,
  `tests/product-peer.test.ts` (tenant isolation включительно). Прежнее
  замечание про PutRow остаётся верным только для внутреннего row-write
  adapter'а; публичный API реализован отдельным слоем.
- [x] Параметризованный SELECT parser/planner → GLinq, immutable snapshot и
  лимиты. Реализация в `internal/application/query.go`,
  `query_lex.go`, `query_parse.go`, `query_validate.go`, `query_eval.go`,
  `query_exec.go`: одиночный SELECT, JOIN только по объявленным `references`,
  агрегаты с GROUP BY/HAVING, DISTINCT/ORDER BY/LIMIT/OFFSET, детерминированный
  порядок по умолчанию. glinq (`github.com/CreateLab/glinq`) даёт
  Where/GroupBy/OrderBy/DistinctBy/Skip/Take; парсер, валидация, nested-loop
  equijoin (в glinq нет join-оператора) и aggregate folds написаны вручную.
  Лимиты: SQL ≤8192 B, параметры ≤64, LIMIT/OFFSET ≤10000 (default 100), scan
  ≤200000 строк → `query_rejected`. Red→green: `tests/query-planner.test.ts`
  (52 теста).
- [x] Согласованная rollback-операция, восстанавливающая pre-migration snapshot с потерей более поздних записей и новым data epoch. Покрытие: `tests/cluster-migration.test.ts`.
- [ ] Plugin SDK REST Reload/pull, strict generation ACK, mTLS и CLI ручного
  запуска. Открыто полностью: plugin-sdk v2
  self-registration/lease/peer-directory API ещё не опубликован, поэтому Reload,
  generation ACK и CLI из этого репозитория не подключались (mTLS самого peer
  transport реализован и покрыт conformance — это другой слой).
- [x] Domain product peer API: 7 методов на общем mTLS endpoint, envelope и
  коды ошибок в `contracts/v1/schemas`; локальный полный Vitest — 16 файлов /
  104 теста, `go build ./...`, `go vet ./...`, `go test ./...` и
  `git diff --check` прошли. Это не означает готовность всего Domain v2.
- [ ] Завершить Core-side integration, Plugin SDK lifecycle/registration,
  release SemVer/digest compatibility и полные macOS/Linux gates. Локальный
  OS-partition прошёл 4/4 на darwin через OrbStack; hosted `ubuntu-24.04`
  required workflow ещё не подтверждён. Не объявлять Domain production-ready до
  прохождения этих сквозных gates.
