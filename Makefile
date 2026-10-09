GOLANGCI_VERSION := v2.12.2
GOLANGCI := .tools/$(GOLANGCI_VERSION)/golangci-lint$(shell go env GOEXE)
GO_PACKAGES := ./cmd/... ./contracts/... ./internal/... ./tests/...
export GOWORK := off
export GOFLAGS := -p=1

.PHONY: check lint lint-go lint-ts generate check-generated

check: check-generated lint
	go test $(GO_PACKAGES) -count=1
	npm test
	go vet $(GO_PACKAGES)
	go build $(GO_PACKAGES)

lint: lint-go lint-ts

$(GOLANGCI):
	GOBIN="$(CURDIR)/.tools/$(GOLANGCI_VERSION)" GOTOOLCHAIN=go1.26.0 go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_VERSION)

lint-go: $(GOLANGCI)
	$(GOLANGCI) config verify --config .golangci.yml
	$(GOLANGCI) run --config .golangci.yml $(GO_PACKAGES)

lint-ts:
	npm run lint
	npm run typecheck

generate:
	go generate ./contracts

check-generated:
	go test ./contracts -count=1
