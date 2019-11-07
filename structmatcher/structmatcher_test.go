package structmatcher_test

import (
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"

	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestStructMatcher(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "structmatcher tests")
}

var _ = Describe("structmatcher.StructMatchers", func() {
	type SimpleStruct struct {
		Field1 int
		Field2 string
	}
	type NestedStruct struct {
		Field1      int
		Field2      string
		NestedSlice []SimpleStruct
	}
	Describe("structmatcher.StructMatcher", func() {
		It("returns no failures for the same structs", func() {
			struct1 := SimpleStruct{Field1: 0, Field2: "message1"}
			struct2 := SimpleStruct{Field1: 0, Field2: "message1"}
			mismatches := structmatcher.StructMatcher(&struct1, &struct2, false, false)
			Expect(mismatches).To(BeEmpty())
		})
		It("returns mismatches with different structs", func() {
			struct1 := SimpleStruct{Field1: 0, Field2: "message1"}
			struct2 := SimpleStruct{Field1: 0, Field2: ""}
			mismatches := structmatcher.StructMatcher(&struct1, &struct2, false, false)
			Expect(mismatches).ToNot(BeEmpty())
		})
		It("formats a nice error message for mismatches", func() {
			struct1 := SimpleStruct{Field1: 0, Field2: "message1"}
			struct2 := SimpleStruct{Field1: 0, Field2: "message2"}
			mismatches := structmatcher.StructMatcher(&struct1, &struct2, false, false)
			Expect(mismatches).To(Equal([]string{"Mismatch on field Field2\nExpected\n    <string>: message2\nto equal\n    <string>: message1"}))
		})
		It("returns mismatches in nested struct slices", func() {
			struct1 := NestedStruct{Field1: 0, Field2: "message1", NestedSlice: []SimpleStruct{{Field1: 3}}}
			struct2 := NestedStruct{Field1: 0, Field2: "message1", NestedSlice: []SimpleStruct{{Field1: 4}}}
			mismatches := structmatcher.StructMatcher(&struct1, &struct2, false, false)
			Expect(len(mismatches)).To(Equal(1))
			Expect(mismatches[0]).To(Equal("Mismatch on field Field1\nExpected\n    <int>: 4\nto equal\n    <int>: 3"))
		})
		It("returns mismatches including struct fields", func() {
			struct1 := NestedStruct{Field1: 0, Field2: "message1", NestedSlice: []SimpleStruct{{Field1: 3}}}
			struct2 := NestedStruct{Field1: 0, Field2: "teststruct2", NestedSlice: []SimpleStruct{{Field1: 4}}}
			mismatches := structmatcher.StructMatcher(&struct1, &struct2, true, true, "Field2")
			Expect(len(mismatches)).To(Equal(1))
			Expect(mismatches[0]).To(Equal("Mismatch on field Field2\nExpected\n    <string>: teststruct2\nto equal\n    <string>: message1"))
		})
		It("returns mismatches including nested struct slice fields", func() {
			struct1 := NestedStruct{Field1: 0, Field2: "message1", NestedSlice: []SimpleStruct{{Field1: 3}}}
			struct2 := NestedStruct{Field1: 0, Field2: "teststruct2", NestedSlice: []SimpleStruct{{Field1: 4}}}
			mismatches := structmatcher.StructMatcher(&struct1, &struct2, true, true, "NestedSlice.Field1")
			Expect(len(mismatches)).To(Equal(1))
			Expect(mismatches[0]).To(Equal("Mismatch on field Field1\nExpected\n    <int>: 4\nto equal\n    <int>: 3"))
		})
		It("returns mismatches excluding struct fields", func() {
			struct1 := NestedStruct{Field1: 0, Field2: "message1", NestedSlice: []SimpleStruct{{Field1: 3}}}
			struct2 := NestedStruct{Field1: 0, Field2: "teststruct2", NestedSlice: []SimpleStruct{{Field1: 4}}}
			mismatches := structmatcher.StructMatcher(&struct1, &struct2, true, false, "Field2")
			Expect(len(mismatches)).To(Equal(1))
			Expect(mismatches[0]).To(Equal("Mismatch on field Field1\nExpected\n    <int>: 4\nto equal\n    <int>: 3"))
		})
		It("returns mismatches excluding nested struct slice fields", func() {
			struct1 := NestedStruct{Field1: 0, Field2: "message1", NestedSlice: []SimpleStruct{{Field1: 3}}}
			struct2 := NestedStruct{Field1: 0, Field2: "teststruct2", NestedSlice: []SimpleStruct{{Field1: 4}}}
			mismatches := structmatcher.StructMatcher(&struct1, &struct2, true, false, "NestedSlice.Field1")
			Expect(len(mismatches)).To(Equal(1))
			Expect(mismatches[0]).To(Equal("Mismatch on field Field2\nExpected\n    <string>: teststruct2\nto equal\n    <string>: message1"))
		})
	})

	var _ = Describe("structmatcher.MatchStruct() GomegaMatcher", func() {
		It("returns no failures for the same structs", func() {
			struct1 := SimpleStruct{Field1: 0, Field2: "message1"}
			struct2 := SimpleStruct{Field1: 0, Field2: "message1"}
			Expect(struct2).To(structmatcher.MatchStruct(struct1))
		})
		It("returns mismatches with different structs", func() {
			struct1 := SimpleStruct{Field1: 0, Field2: "message1"}
			struct2 := SimpleStruct{Field1: 0, Field2: ""}
			Expect(struct2).NotTo(structmatcher.MatchStruct(struct1))
		})
		It("formats a nice error message for mismatches", func() {
			struct1 := SimpleStruct{Field1: 0, Field2: "message1"}
			struct2 := SimpleStruct{Field1: 0, Field2: "message2"}
			messages := InterceptGomegaFailures(func() {
				Expect(struct2).To(structmatcher.MatchStruct(struct1))
			})
			Expect(messages).To(Equal([]string{"Expected structs to match but:\nMismatch on field Field2\nExpected\n    <string>: message2\nto equal\n    <string>: message1"}))
		})
		It("returns mismatches in nested struct slices", func() {
			struct1 := NestedStruct{Field1: 0, Field2: "message1", NestedSlice: []SimpleStruct{{Field1: 3}}}
			struct2 := NestedStruct{Field1: 0, Field2: "message1", NestedSlice: []SimpleStruct{{Field1: 4}}}
			messages := InterceptGomegaFailures(func() {
				Expect(struct2).To(structmatcher.MatchStruct(struct1))
			})
			Expect(messages).To(Equal([]string{"Expected structs to match but:\nMismatch on field Field1\nExpected\n    <int>: 4\nto equal\n    <int>: 3"}))
		})
		It("returns mismatches including struct fields", func() {
			struct1 := NestedStruct{Field1: 0, Field2: "message1", NestedSlice: []SimpleStruct{{Field1: 3}}}
			struct2 := NestedStruct{Field1: 0, Field2: "teststruct2", NestedSlice: []SimpleStruct{{Field1: 4}}}
			messages := InterceptGomegaFailures(func() {
				Expect(struct2).To(structmatcher.MatchStruct(struct1).IncludingFields("Field2"))
			})
			Expect(messages).To(Equal([]string{"Expected structs to match but:\nMismatch on field Field2\nExpected\n    <string>: teststruct2\nto equal\n    <string>: message1"}))
		})
		It("returns mismatches including nested struct slice fields", func() {
			struct1 := NestedStruct{Field1: 0, Field2: "message1", NestedSlice: []SimpleStruct{{Field1: 3}}}
			struct2 := NestedStruct{Field1: 0, Field2: "teststruct2", NestedSlice: []SimpleStruct{{Field1: 4}}}
			messages := InterceptGomegaFailures(func() {
				Expect(struct2).To(structmatcher.MatchStruct(struct1).IncludingFields("NestedSlice.Field1"))
			})
			Expect(messages).To(Equal([]string{"Expected structs to match but:\nMismatch on field Field1\nExpected\n    <int>: 4\nto equal\n    <int>: 3"}))
		})
		It("returns mismatches excluding struct fields", func() {
			struct1 := NestedStruct{Field1: 0, Field2: "message1", NestedSlice: []SimpleStruct{{Field1: 3}}}
			struct2 := NestedStruct{Field1: 0, Field2: "teststruct2", NestedSlice: []SimpleStruct{{Field1: 4}}}
			messages := InterceptGomegaFailures(func() {
				Expect(struct2).To(structmatcher.MatchStruct(struct1).ExcludingFields("Field2"))
			})
			Expect(messages).To(Equal([]string{"Expected structs to match but:\nMismatch on field Field1\nExpected\n    <int>: 4\nto equal\n    <int>: 3"}))
		})
		It("returns mismatches excluding nested struct slice fields", func() {
			struct1 := NestedStruct{Field1: 0, Field2: "message1", NestedSlice: []SimpleStruct{{Field1: 3}}}
			struct2 := NestedStruct{Field1: 0, Field2: "teststruct2", NestedSlice: []SimpleStruct{{Field1: 4}}}
			messages := InterceptGomegaFailures(func() {
				Expect(struct2).To(structmatcher.MatchStruct(struct1).ExcludingFields("NestedSlice.Field1"))
			})
			Expect(messages).To(Equal([]string{"Expected structs to match but:\nMismatch on field Field2\nExpected\n    <string>: teststruct2\nto equal\n    <string>: message1"}))
		})

		It("gives a negated failure message", func() {
			struct1 := SimpleStruct{Field1: 0, Field2: "message1"}
			struct2 := SimpleStruct{Field1: 0, Field2: "message1"}
			messages := InterceptGomegaFailures(func() {
				Expect(struct2).NotTo(structmatcher.MatchStruct(struct1))
			})
			Expect(messages).To(Equal([]string{"Expected structs not to match, but they did"}))
		})
	})
})
