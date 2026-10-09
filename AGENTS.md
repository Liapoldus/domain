# AGENTS.md — Domain

Репозиторий владеет ER-моделью, хранением данных, миграциями, аналитикой и
собственными peer-контрактами. Не добавлять методы продукта в Core, Plugin SDK
или `pluginprotocol`.

Соблюдать четыре production-слоя: `internal/domain/{models,interfaces}`
(только модели и порты), плоский `internal/application`, тематические
`internal/infrastructure` и `internal/presentation`. Composition размещать в
`cmd/`. Публичные схемы и ошибки версионировать в `contracts/v1/`, не копировать
их в другие репозитории.

Детерминированные Go unit/integration tests размещать рядом с кодом в
`*_test.go`; TypeScript/Vitest child-process и E2E tests — в `tests/`.
Сначала красный тест, затем реализация и зелёный срез. Перед передачей работы
выполнить `go test ./...`, весь Vitest, `go build ./...` и `go vet ./...`;
не называть непроверенный gate успешным.

SQL принадлежит SQLite-адаптеру: именованные Go queries в
`internal/infrastructure/sqlite/queries.go`, без внешних `.sql` assets и loader.
При переносе сохранять точные байты SQL, порядок операторов и привязку
параметров; не переписывать schema, migration/rollback или snapshot semantics.
`queries_test.go` фиксирует SHA-256, длины и placeholders исходного SQL-среза.

Не выводить в логи и ошибки данные строк, SQL-параметры, credentials и секреты
tenant. Сохранять чужие незакоммиченные файлы. Не делать push, tag и публикацию
без запроса пользователя.
