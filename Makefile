test:
	GO111MODULE=on go test -p 1 -v -race ./
	go vet ./...

fmt: ## Run go fmt against code.
	go fmt ./...

vet: ## Run go vet against code.
	go vet ./...

local-ci:
	make -C tests docker-compose

ci:
	go test -v ./tests/...
