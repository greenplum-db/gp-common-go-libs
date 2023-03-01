all: depend test

SHELL := /bin/bash
.DEFAULT_GOAL := all
DIR_PATH=$(shell dirname `pwd`)
BIN_DIR=$(shell echo $${GOPATH:-~/go} | awk -F':' '{ print $$1 "/bin"}')
BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)
GOLANG_VERSION = 1.19.6
GINKGO=$(GOPATH)/bin/ginkgo
DEST = .

GOFLAGS :=

.PHONY: test lint goimports golangci-lint gofmt unit coverage depend set-dev set-prod

test: lint unit

lint:
	$(MAKE) goimports gofmt golangci-lint

goimports:
	docker run --rm -i -v "${PWD}":/data -w /data unibeautify/goimports -w -l /data

golangci-lint:
	docker run --rm -v ${PWD}:/data -w /data golangci/golangci-lint golangci-lint run -v

gofmt:
	docker run --rm -v ${PWD}:/data cytopia/gofmt --ci .

$(GINKGO):
	go install github.com/onsi/ginkgo/v2/ginkgo@latest

unit: $(GINKGO)
		ginkgo -r --keep-going --randomize-suites --randomize-all cluster dbconn gplog iohelper structmatcher conv 2>&1

coverage :
		@./show_coverage.sh

depend:
	go mod download

clean :
		# Test artifacts
		rm -rf /tmp/go-build*
		rm -rf /tmp/gexec_artifacts*
		rm -rf /tmp/ginkgo*
		# Code coverage files
		rm -rf /tmp/cover*
		rm -rf /tmp/unit*

##### Pipeline targets #####

set-dev:
	fly --target dev set-pipeline --check-creds \
	--pipeline=dev-gp-common-go-libs-${BRANCH}-${USER} \
	-c ci/pipeline.yml \
	--var=branch=${BRANCH} \
	--var=golang-version=${GOLANG_VERSION}

set-prod:
	fly --target prod set-pipeline --check-creds \
	--pipeline=gp-common-go-libs \
	-c ci/pipeline.yml \
	--var=branch=main\
	--var=golang-version=${GOLANG_VERSION}
