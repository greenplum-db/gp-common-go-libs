package conv

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"testing"
)

func TestMutils(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Conv Suite")
}
