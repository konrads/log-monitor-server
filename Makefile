validate-tools:
	@docker -v
	@go version
	@echo Required tools present

bootstrap-docker:
	docker pull mariadb:10.6

test:
	go test -v ./...

test-it:
	go clean -testcache && go test ./... -p 1 -timeout 150s -v -tags=it