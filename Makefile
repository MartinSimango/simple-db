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