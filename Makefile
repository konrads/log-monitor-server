validate-tools:
	docker -v > /dev/null || echo No docker installed
	go version > /dev/null || echo No go installed

bootstrap-docker:
	docker pull mariadb:10.6

test:
	go test -v ./...

test-it:
	go clean -testcache && go test ./... -p 1 -timeout 150s -v -tags=it