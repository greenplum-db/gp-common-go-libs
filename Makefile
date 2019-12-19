all: depend test

SHELL := /bin/bash
.DEFAULT_GOAL := all
DIR_PATH=$(shell dirname `pwd`)
BIN_DIR=$(shell echo $${GOPATH:-~/go} | awk -F':' '{ print $$1 "/bin"}')

DEST = .

GOFLAGS :=

.PHONY : coverage

dependencies :
		curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.21.0
		go get golang.org/x/tools/cmd/goimports
		go get github.com/onsi/ginkgo/ginkgo
		go mod vendor
		go mod tidy

format :
		goimports -w .
		gofmt -w -s .

lint :
		! gofmt -l structmatcher/ | read
		golangci-lint run -v

unit :
		ginkgo -r -keepGoing -randomizeSuites -randomizeAllSpecs cluster dbconn gplog iohelper structmatcher 2>&1

test : lint unit

coverage :
		@./show_coverage.sh

depend : dependencies

clean :
		# Test artifacts
		rm -rf /tmp/go-build*
		rm -rf /tmp/gexec_artifacts*
		rm -rf /tmp/ginkgo*
		# Code coverage files
		rm -rf /tmp/cover*
		rm -rf /tmp/unit*
