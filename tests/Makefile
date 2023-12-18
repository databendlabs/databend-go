TEST_DATABEND_DSN ?= "databend://databend:databend@localhost:8000/default?sslmode=disable"

up:
	mkdir -p data/databend
	docker compose up --quiet-pull -d databend --wait

down:
	docker compose down

integration: up
	TEST_DATABEND_DSN=${TEST_DATABEND_DSN} go test -v -timeout 30m ./...
