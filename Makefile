TEST_DATABEND_DSN ?= "databend://databend:databend@localhost:8000/default?sslmode=disable"

test:
	GO111MODULE=on go test -p 1 -v -race ./
	go vet ./...

fmt: ## Run go fmt against code.
	go fmt ./...

vet: ## Run go vet against code.
	go vet ./...

integration:
	make -C tests integration

compat:
	make -C tests compat
