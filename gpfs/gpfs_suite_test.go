package gpfs_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestGpfsSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "gpfs Suite")
}
