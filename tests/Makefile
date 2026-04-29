TEST_DATABEND_DSN ?= databend://databend:databend@localhost:8000/default?sslmode=disable
TEST_QUERY_RESULT_FORMAT ?= json
TEST_SKIP_CASES ?=
GO_TEST_SKIP_ARG := $(if $(strip $(TEST_SKIP_CASES)),-skip '$(TEST_SKIP_CASES)',)

ifeq ($(TEST_QUERY_RESULT_FORMAT),arrow)
ifneq ($(findstring query_result_format=,$(TEST_DATABEND_DSN)),)
INTEGRATION_TEST_DSN := $(TEST_DATABEND_DSN)
else ifneq ($(findstring ?,$(TEST_DATABEND_DSN)),)
INTEGRATION_TEST_DSN := $(TEST_DATABEND_DSN)&query_result_format=arrow
else
INTEGRATION_TEST_DSN := $(TEST_DATABEND_DSN)?query_result_format=arrow
endif
else
INTEGRATION_TEST_DSN := $(TEST_DATABEND_DSN)
endif

prepare:
	mkdir -p data/databend

up: prepare
	docker compose up --scale query=3 --quiet-pull -d  --wait
	grep -q '127.0.0.1 minio' /etc/hosts || echo '127.0.0.1 minio' | sudo tee -a /etc/hosts > /dev/null
	curl  -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "select version()",  "pagination": { "wait_time_secs": 10}}'


down:
	docker compose down

integration: up
	TEST_DATABEND_DSN='$(INTEGRATION_TEST_DSN)' go test -v -timeout 30m $(GO_TEST_SKIP_ARG) ./...
