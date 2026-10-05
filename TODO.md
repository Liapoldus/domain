# Domain — задачи общего v2

Начальные локальные срезы сохранены как прототип. Все незакрытые пункты ниже
относятся к общему релизу Liapoldus v2 и не входят в приёмку v1. Версия
`contracts/v1` обозначает первую версию контракта самого плагина, а не этап
экосистемы.

- [x] Версионированная начальная ER-схема и тестируемый план безопасных миграций.
- [x] Начальный product Manifest с собственными методами; payload/response/error schemas и runtime registration ещё нужны.
- [ ] Проверка всей JSON Schema, связей/ограничений и явных type conversions на данных.
- [x] Локальное атомарное применение migration plan к SQLite с preflight, rollback транзакции при ошибке и проверкой после reopen.
- [x] Локальный SQLite pre-migration snapshot и rollback с удалением post-migration записей из активного состояния.
- [ ] Согласовать эти операции через Raft FSM, добавить crash recovery, epoch fencing и audit; локальный adapter не является источником quorum.
- [x] Тестовая трёхузловая Raft-сборка на in-memory transport: leader election, majority commit, failover и отказ записи без quorum.
- [x] SQLite FSM apply/snapshot/restore через Hashicorp Raft interfaces; подтверждено локальным тестом.
- [ ] Production durable Raft LogStore/StableStore и transport adapter через `pluginprotocol` с per-node mTLS; in-memory transport только для tests.
- [ ] Bounded streaming snapshots и crash recovery; текущий FSM экспортирует snapshot в память и не рассчитан на большие datasets.
- [ ] Свежие read barriers, fencing при потере quorum, идемпотентность и unknown-outcome writes.
- [ ] Product-owned peer CRUD, ACID batch, tenant/site ACL, ER constraints и проверка identity caller. Текущий SQLite PutRow — только внутренний FSM/test adapter, не публичный API.
- [ ] Параметризованный SELECT parser/planner → GLinq, immutable snapshot и лимиты.
- [ ] Согласованная rollback-операция, восстанавливающая pre-migration snapshot с потерей более поздних записей и новым data epoch.
- [ ] Plugin SDK REST Reload/pull, strict generation ACK, mTLS и CLI ручного запуска.
- [ ] 3-node fault-injection, end-to-end и macOS/Linux gates; не объявлять плагин готовым до их прохождения.
