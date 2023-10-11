package gpssh_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestGpSsh(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "gpssh Suite")
}
