db-vet:

db-build:
	go build -o sdb cmd/sdb/main.go
# 	go build -o sdb-client cmd/sdb-client/main.go

db-install: db-build
	go install ./cmd/sdb

# 	go install ./cmd/sdb-client

run-db: db-install
	sdb
run-db-quick: 
	go run cmd/sdb/main.go

run-client:
	go run ./cmd/sdb-client/main.go put localhost "Hello, World!"


build-db-test:
	go build -o sdb-test cmd/sdb-test/main.go
