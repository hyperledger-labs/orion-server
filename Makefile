TIMEOUT  = 20m
GO      = go
DOCKER  = docker
DOCKER_IMAGE = orionbcdb/orion-server
DOCKERFILE = images/Dockerfile
PROTO_COMPILER_IMAGE = orionbcdb/protobuf
PROTO_COMPILER_DOCKERFILE = images/Proto
PKGS     = $(or $(PKG),$(shell env GO111MODULE=on $(GO) list ./...))
TESTPKGS = $(shell env GO111MODULE=on $(GO) list -f \
		   '{{ if or .TestGoFiles .XTestGoFiles }}{{ .ImportPath }}{{ end }}' \
		   $(PKGS))

COVERAGE_MODE    = atomic
COVERAGE_PROFILE = $(COVERAGE_DIR)/profile.out
COVERAGE_XML     = $(COVERAGE_DIR)/coverage.xml
COVERAGE_HTML    = $(COVERAGE_DIR)/index.html
BIN = $(CURDIR)/bin

$(BIN):
	@mkdir -p $@

$(BIN)/%: | $(BIN)
	@tmp=$$(mktemp -d); \
		env GO11MODULE=off GOPATH=$$tmpp GOBIN=$(BIN) go get $(PACKAGE) \
		|| ret=$$?;
	rm -rf $$tmp ; exit $$ret

$(BIN)/golangci-lint: PACKAGE=github.com/golangci/golangci-lint/cmd/golangci-lint

GOLINT = $(BIN)/golangci-lint

lint: | $(GOLINT)
	$(GOLINT) run  

GOCOV = $(BIN)/gocov
$(BIN)/gocov: PACKAGE=github.com/axw/gocov/...

GOCOVXML = $(BIN)/gocov-xml
$(BIN)/gocov-xml: PACKAGE=github.com/AlekSi/gocov-xml

GO2XUNIT = $(BIN)/go2xunit
$(BIN)/go2xunit: PACKAGE=github.com/tebeka/go2xunit

.PHONY: fmt
fmt:
	$(GO) fmt $(PKGS)

.PHONY: goimports
goimports:
	find . -name \*.go -not -path "./pkg/types/*" -exec goimports -w -l {} \;

.PHONY: binary
binary:
	go build -o $(BIN)/bdb cmd/bdb/main.go
	go build -o $(BIN)/signer cmd/signer/signer.go
	go build -o $(BIN)/encoder cmd/base64_encoder/encoder.go
	go build -o $(BIN)/decoder cmd/base64_decoder/decoder.go

.PHONY: test
test-script: 
	scripts/run-unit-tests.sh

.PHONY: clean
clean: 
	@rm -rf $(BIN)
	@rm -rf test/tests.* test/coverage.*

.PHONY: docker-clean
docker-clean:
	$(DOCKER) rmi $(DOCKER_IMAGE)

.PHONY: protos
protos:
	$(DOCKER) build -t $(PROTO_COMPILER_IMAGE) --no-cache -f $(PROTO_COMPILER_DOCKERFILE) .
	docker run -it -v `pwd`:`pwd` -w `pwd` $(PROTO_COMPILER_IMAGE) scripts/compile_go_protos.sh

TEST_TARGETS := test-default test-bench test-short test-verbose test-race
test-bench:   ARGS=-run=__absolutelynothing__ -bench=.
test-short:   ARGS=-short
test-verbose: ARGS=-v
test-race:    ARGS=-race
$(TEST_TARGETS): test
check test tests:
	go build -o $(BIN)/bdb cmd/bdb/main.go
	go test -timeout $(TIMEOUT) $(ARGS) $(TESTPKGS)

test-coverage-tools: | $(GOCOVMERGE) $(GOCOV) $(GOCOVXML) 
test-coverage: COVERAGE_DIR := $(CURDIR)/test/coverage.$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
test-coverage: test-coverage-tools
	go build -o $(BIN)/bdb cmd/bdb/main.go
	mkdir -p $(COVERAGE_DIR)/coverage
	$(GO) test \
		-coverpkg=$$($(GO) list -f '{{ join .Deps "\n" }}' $(TESTPKGS) | \
		grep '^$(MODULE)/' | \
		tr '\n' ',' | sed 's/,$$//') \
		-covermode=$(COVERAGE_MODE) \
		-coverprofile="$(COVERAGE_PROFILE)" $(TESTPKGS)
	$(GO) tool cover -html=$(COVERAGE_PROFILE) -o $(COVERAGE_HTML)
	$(GOCOV) convert $(COVERAGE_PROFILE) | $(GOCOVXML) > $(COVERAGE_XML)

.PHONY: docker
docker:
	$(DOCKER) build -t $(DOCKER_IMAGE) --no-cache -f $(DOCKERFILE) .

.PHONY: docker-multiarch
docker-multiarch:
	make docker-linux-amd64
	make docker-linux-arm64
	make docker-linux-arm-v7
	make docker-linux-s390x
	
.PHONY: docker-linux-amd64
docker-linux-amd64:
	$(DOCKER) buildx build --platform linux/amd64 -f $(DOCKERFILE) -t $(DOCKER_IMAGE):linux-amd64 --load .

.PHONY: docker-linux-arm64
docker-linux-arm64:
	$(DOCKER) buildx build --platform linux/arm64 -f $(DOCKERFILE) -t $(DOCKER_IMAGE):linux-arm64 --load .

.PHONY: docker-linux-arm-v7
docker-linux-arm-v7:
	$(DOCKER) buildx build --platform linux/arm/v7 -f $(DOCKERFILE) -t $(DOCKER_IMAGE):linux-arm-v7 --load .

.PHONY: docker-linux-s390x
docker-linux-s390x:
	$(DOCKER) buildx build --platform linux/s390x -f $(DOCKERFILE) -t $(DOCKER_IMAGE):linux-s390x --load .

