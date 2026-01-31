# Build binaries	
sdb-build:
	go build -o sdb cmd/sdb/main.go

sdb-cli-build:
	go build -o sdb-cli cmd/sdb-cli/main.go

sdb-test-build:
	go build -o sdb-test cmd/sdb-test/main.go


# Install binaries
sdb-install: 
	go install ./cmd/sdb

sdb-cli-install:
	go install ./cmd/sdb-cli


sdb-test-install:
	go install ./cmd/sdb-test


# Run binaries
sdb-run: 
	go run cmd/sdb/main.go


$(GOPATH)/bin/pkgsite: 
	go install golang.org/x/pkgsite/cmd/pkgsite@latest


pkgsite: $(GOPATH)/bin/pkgsite
	pkgsite -http=:8080 


### TESTING

.PHONY: test
test:
	go test -v ./...

test-pretty:
	go test ./...  -json | tparse -all

test-integration:
	go test -tags=integration ./internal/...

test-integration-pretty:
	go test -tags=integration ./internal/...  -json | tparse -all


coverage:
	go test -v -cover -coverprofile=c.out ./...

coverage-html:
	go tool cover -html=c.out -o coverage.html -func=c.out

coverage-func:
	go tool cover -func=c.out