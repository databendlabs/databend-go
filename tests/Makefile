TEST_DATABEND_DSN ?= "databend://databend:databend@localhost:8000/default?sslmode=disable"
TEST_QUERY_RESULT_FORMAT ?= "json"

COMPAT_TEST_TAGS := $(strip $(shell python3 scripts/compat_tags.py))

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
	TEST_DATABEND_DSN='$(INTEGRATION_TEST_DSN)' go test -v -timeout 30m ./...


compat: up
	test -f go.mod || go mod init it
	go get github.com/datafuselabs/databend-go@${DATABEND_GO_VERSION}
	go get -t ./
	TEST_DATABEND_DSN=${TEST_DATABEND_DSN} go test $(COMPAT_TEST_TAGS) -v -timeout 30m ./
