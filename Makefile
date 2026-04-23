TEST_DATABEND_DSN ?= "databend://databend:databend@localhost:8000/default?sslmode=disable"
TEST_QUERY_RESULT_FORMAT ?= "json"

test:
	GO111MODULE=on go test -p 1 -v -race ./
	go vet ./...

fmt: ## Run go fmt against code.
	go fmt ./...

vet: ## Run go vet against code.
	go vet ./...

integration:
	make -C tests integration TEST_DATABEND_DSN=${TEST_DATABEND_DSN} TEST_QUERY_RESULT_FORMAT=${TEST_QUERY_RESULT_FORMAT}

compat:
	make -C tests compat
