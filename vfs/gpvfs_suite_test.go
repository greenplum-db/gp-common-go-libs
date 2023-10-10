package vfs_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestConfigure(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Greenplum Filesystem Suite")
}
