package conv

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestMutils(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Conv Suite")
}
