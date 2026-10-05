# Domain

Отдельный Liapoldus plugin для ER-модели и данных. Это прототип для общего v2,
не часть приёмки v1 и не production release. Цель и текущие разрывы описаны в
[документации](docs/site/plugins/domain.md) и [TODO](TODO.md).

Проверки: `npm ci && npm test`, `go build ./...`, `go vet ./...`.
Локальный Go workspace подключает модуль через `plugins/go.work`; независимые
проверки можно выполнять с `GOWORK=off`.
