# Log Monitor Server
Log aggregator that batches the incoming messages, persists batches periodically.

Folder structure:
```
*
|
+--+ pkg
|  |
|  +-- data        -  data access layer
|  |
|  +-- model       -  data object layer
|  |
|  +-- util        -  useful utils
|
+-- it             -  IT tests
|
+-- migrations     -  db setup/teardown scripts
```

## Features
* Goroutine based batching and persisting mechanism, as per [dcl.go](pkg/dcl/dcl.go). Note: this should contain a `main()` but needs incoming streaming
* MySQL db store that persists message batch and severity stats, as per [message_reader.go](pkg/data/message_reader.go) and [message_writer.go](pkg/data/message_writer.go)
* Unit tests
* Integration tests that utilize docker for MySQL storage, as per [e2e_test](it/e2e_test.go)

## TBD
* Incoming streaming (via [pstest](https://pkg.go.dev/cloud.google.com/go/pubsub@v1.9.1/pstest)) has not been implemented
* Improve testing, due to time constraints project only contains rudimentary testing

## Run:
```bash
make validate-tools    # validate existence of appropriate tools such as docker, go
make bootstrap-docker  # pre-fetch required docker images
make test              # run unit tests
make test-it           # run integration tests, note some take ~2mins
```