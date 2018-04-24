all: depend build test

SHELL := /bin/bash
.DEFAULT_GOAL := all
DIR_PATH=$(shell dirname `pwd`)
BIN_DIR=$(shell echo $${GOPATH:-~/go} | awk -F':' '{ print $$1 "/bin"}')

DEST = .

GOFLAGS :=

.PHONY : coverage

dependencies :
		go get github.com/alecthomas/gometalinter
		gometalinter --install
		go get github.com/golang/dep/cmd/dep
		dep ensure
		@cd vendor/golang.org/x/tools/cmd/goimports; go install .
		@cd vendor/github.com/onsi/ginkgo/ginkgo; go install .

format :
		goimports -w .
		gofmt -w -s .

lint :
		! gofmt -l structmatcher/ | read
		gometalinter --config=gometalinter.config -s vendor ./...

unit :
		ginkgo -r -randomizeSuites -randomizeAllSpecs dbconn structmatcher gplog cluster 2>&1

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
