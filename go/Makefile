.PHONY: all build demo tui test test-v race cover bench vet fmt tidy clean deb deb-clean

GO       ?= go
BIN_DIR  := bin
PKGS     := ./...
VERSION  ?= 0.1.1
ARCH     ?= $(shell dpkg --print-architecture 2>/dev/null || echo amd64)

all: build

build:
	mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/demo ./cmd/demo
	$(GO) build -o $(BIN_DIR)/tui  ./cmd/tui

demo: build
	$(BIN_DIR)/demo

tui: build
	$(BIN_DIR)/tui

test:
	$(GO) test $(PKGS)

test-v:
	$(GO) test -v $(PKGS)

race:
	$(GO) test -race $(PKGS)

cover:
	$(GO) test -cover -coverprofile=coverage.out $(PKGS)
	$(GO) tool cover -func=coverage.out | tail -20

bench:
	$(GO) test -run=^$$ -bench=. -benchmem ./sharding/...

vet:
	$(GO) vet $(PKGS)

fmt:
	$(GO) fmt $(PKGS)

tidy:
	$(GO) mod tidy

clean:
	rm -rf $(BIN_DIR) coverage.out

deb:
	./scripts/build-deb.sh $(VERSION) $(ARCH)

deb-clean:
	rm -rf dist
