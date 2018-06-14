package activerecord_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestActiveRecord(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Active Record Suite")
}
