# Log Monitor Server
Log aggregator that batches the incoming messages, persists batches periodically.

Folder structure:
```
|
+--+ cmd
|  |
|  +-- dcl         -  orchestration of the DCL layer
|
+--+ pkg
|  |
|  +-- data        -  data access layer
|  |
|  +-- model       -  data object layer
|  |
|  +-- util        -  useful utils
|  |
|  +-- it          -  IT tests
|  |
+--+-- migrations  -  db setup/teardown scripts
```

## Features
* Goroutine based batching and persisting mechanism
* MySQL db store that persists message batch and severity stats
* Unit tests
* Integration tests that utilize docker for MySQL storage

## TBD
* Incoming streaming has not been implemented

## Run:
```bash
make validate-tools    # validate existance of appropriate tools
make bootstrap-docker  # pre-fetch required docker images
make test              # run unit tests
make test-it           # run integration tests, note some take ~2mins
```