BIN := ch-sync
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo 1.0.0)
LDFLAGS := -s -w
DIST := dist
CONFIG := config.yaml
TABLES := tables.yaml

.PHONY: all tidy build build-mac build-linux package-mac package-linux release clean

all: build

tidy:
	go mod tidy

build:
	mkdir -p bin
	go build -ldflags "$(LDFLAGS)" -o bin/$(BIN) .
	cp $(CONFIG) bin/
	- cp $(TABLES) bin/ 2>/dev/null || true

build-mac:
	mkdir -p $(DIST)/$(BIN)-darwin-arm64
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(DIST)/$(BIN)-darwin-arm64/$(BIN) .

build-linux:
	mkdir -p $(DIST)/$(BIN)-linux-amd64
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(DIST)/$(BIN)-linux-amd64/$(BIN) .

package-mac: build-mac
	cp $(CONFIG) $(DIST)/$(BIN)-darwin-arm64/
	- cp $(TABLES) $(DIST)/$(BIN)-darwin-arm64/ 2>/dev/null || true
	- cp README.md $(DIST)/$(BIN)-darwin-arm64/ 2>/dev/null || true
	cd $(DIST) && tar -czf $(BIN)-darwin-arm64-$(VERSION).tar.gz $(BIN)-darwin-arm64

package-linux: build-linux
	cp $(CONFIG) $(DIST)/$(BIN)-linux-amd64/
	- cp $(TABLES) $(DIST)/$(BIN)-linux-amd64/ 2>/dev/null || true
	- cp README.md $(DIST)/$(BIN)-linux-amd64/ 2>/dev/null || true
	cd $(DIST) && tar -czf $(BIN)-linux-amd64-$(VERSION).tar.gz $(BIN)-linux-amd64

release: package-mac package-linux
	@echo "Release artifacts in $(DIST):"
	ls -lh $(DIST)

clean:
	rm -rf bin $(DIST)
