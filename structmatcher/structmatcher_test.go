package structmatcher_test

import (
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"

	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestStructMatcher(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "structmatcher tests")
}

var _ = Describe("structmatcher.StructMatcher", func() {
	type SimpleStruct struct {
		Field1 int
		Field2 string
	}
	type NestedStruct struct {
		Field1      int
		Field2      string
		NestedSlice []SimpleStruct
		Struct      SimpleStruct
		PtrStruct   *SimpleStruct
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
			Expect(mismatches[0]).To(Equal("Mismatch on field NestedSlice[0].Field1\nExpected\n    <int>: 4\nto equal\n    <int>: 3"))
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
			Expect(mismatches[0]).To(Equal("Mismatch on field NestedSlice[0].Field1\nExpected\n    <int>: 4\nto equal\n    <int>: 3"))
		})
		It("returns mismatches excluding struct fields", func() {
			struct1 := NestedStruct{Field1: 0, Field2: "message1", NestedSlice: []SimpleStruct{{Field1: 3}}}
			struct2 := NestedStruct{Field1: 0, Field2: "teststruct2", NestedSlice: []SimpleStruct{{Field1: 4}}}
			mismatches := structmatcher.StructMatcher(&struct1, &struct2, true, false, "Field2")
			Expect(len(mismatches)).To(Equal(1))
			Expect(mismatches[0]).To(Equal("Mismatch on field NestedSlice[0].Field1\nExpected\n    <int>: 4\nto equal\n    <int>: 3"))
		})
		It("returns mismatches excluding nested struct slice fields", func() {
			struct1 := NestedStruct{Field1: 0, Field2: "message1", NestedSlice: []SimpleStruct{{Field1: 3}}}
			struct2 := NestedStruct{Field1: 0, Field2: "teststruct2", NestedSlice: []SimpleStruct{{Field1: 4}}}
			mismatches := structmatcher.StructMatcher(&struct1, &struct2, true, false, "NestedSlice.Field1")
			Expect(len(mismatches)).To(Equal(1))
			Expect(mismatches[0]).To(Equal("Mismatch on field Field2\nExpected\n    <string>: teststruct2\nto equal\n    <string>: message1"))
		})
		It("returns mismatches in nested structs", func() {
			struct1 := NestedStruct{Struct: SimpleStruct{Field1: 7}}
			struct2 := NestedStruct{Struct: SimpleStruct{Field1: 8}}
			mismatches := structmatcher.StructMatcher(&struct1, &struct2, false, false)
			Expect(len(mismatches)).To(Equal(1))
			Expect(mismatches[0]).To(Equal("Mismatch on field Struct.Field1\nExpected\n    <int>: 8\nto equal\n    <int>: 7"))
		})
		It("returns mismatches in nested pointers to structs", func() {
			struct1 := NestedStruct{PtrStruct: &SimpleStruct{Field1: 7}}
			struct2 := NestedStruct{PtrStruct: &SimpleStruct{Field1: 8}}
			mismatches := structmatcher.StructMatcher(&struct1, &struct2, false, false)
			Expect(len(mismatches)).To(Equal(1))
			Expect(mismatches[0]).To(Equal("Mismatch on field PtrStruct.Field1\nExpected\n    <int>: 8\nto equal\n    <int>: 7"))
		})
	})

	Describe("structmatcher.MatchStruct() GomegaMatcher", func() {
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
			Expect(messages).To(Equal([]string{"Expected structs to match but:\nMismatch on field NestedSlice[0].Field1\nExpected\n    <int>: 4\nto equal\n    <int>: 3"}))
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
			Expect(messages).To(Equal([]string{"Expected structs to match but:\nMismatch on field NestedSlice[0].Field1\nExpected\n    <int>: 4\nto equal\n    <int>: 3"}))
		})
		It("returns mismatches excluding struct fields", func() {
			struct1 := NestedStruct{Field1: 0, Field2: "message1", NestedSlice: []SimpleStruct{{Field1: 3}}}
			struct2 := NestedStruct{Field1: 0, Field2: "teststruct2", NestedSlice: []SimpleStruct{{Field1: 4}}}
			messages := InterceptGomegaFailures(func() {
				Expect(struct2).To(structmatcher.MatchStruct(struct1).ExcludingFields("Field2"))
			})
			Expect(messages).To(Equal([]string{"Expected structs to match but:\nMismatch on field NestedSlice[0].Field1\nExpected\n    <int>: 4\nto equal\n    <int>: 3"}))
		})
		It("returns mismatches excluding nested struct slice fields", func() {
			struct1 := NestedStruct{Field1: 0, Field2: "message1", NestedSlice: []SimpleStruct{{Field1: 3}}}
			struct2 := NestedStruct{Field1: 0, Field2: "teststruct2", NestedSlice: []SimpleStruct{{Field1: 4}}}
			messages := InterceptGomegaFailures(func() {
				Expect(struct2).To(structmatcher.MatchStruct(struct1).ExcludingFields("NestedSlice.Field1"))
			})
			Expect(messages).To(Equal([]string{"Expected structs to match but:\nMismatch on field Field2\nExpected\n    <string>: teststruct2\nto equal\n    <string>: message1"}))
		})
		It("returns mismatches in nested structs", func() {
			struct1 := NestedStruct{Struct: SimpleStruct{Field1: 7}}
			struct2 := NestedStruct{Struct: SimpleStruct{Field1: 8}}
			messages := InterceptGomegaFailures(func() {
				Expect(struct2).To(structmatcher.MatchStruct(struct1))
			})
			Expect(messages).To(Equal([]string{"Expected structs to match but:\nMismatch on field Struct.Field1\nExpected\n    <int>: 8\nto equal\n    <int>: 7"}))
		})
		It("returns mismatches in nested pointers to structs", func() {
			struct1 := NestedStruct{PtrStruct: &SimpleStruct{Field1: 7}}
			struct2 := NestedStruct{PtrStruct: &SimpleStruct{Field1: 8}}
			messages := InterceptGomegaFailures(func() {
				Expect(struct2).To(structmatcher.MatchStruct(struct1))
			})
			Expect(messages).To(Equal([]string{"Expected structs to match but:\nMismatch on field PtrStruct.Field1\nExpected\n    <int>: 8\nto equal\n    <int>: 7"}))
		})
		It("can compare nil pointers", func() {
			struct1 := NestedStruct{PtrStruct: nil}
			struct2 := NestedStruct{PtrStruct: &SimpleStruct{Field1: 8}}
			messages := InterceptGomegaFailures(func() {
				Expect(struct2).To(structmatcher.MatchStruct(struct1))
			})
			Expect(messages).To(HaveLen(1))
			Expect(messages[0]).To(MatchRegexp(`Expected structs to match but:\nMismatch on field PtrStruct\nExpected\n    <\*structmatcher_test\.SimpleStruct \| 0x[0-9a-f]+>: \{Field1: 8, Field2: ""}\nto equal\n    <\*structmatcher_test\.SimpleStruct \| 0x0>: nil`))
		})
		It("can compare nil pointers", func() {
			struct1 := NestedStruct{PtrStruct: &SimpleStruct{Field1: 7}}
			struct2 := NestedStruct{PtrStruct: nil}
			messages := InterceptGomegaFailures(func() {
				Expect(struct2).To(structmatcher.MatchStruct(struct1))
			})
			Expect(messages).To(HaveLen(1))
			Expect(messages[0]).To(MatchRegexp(`Expected structs to match but:\nMismatch on field PtrStruct\nExpected\n    <\*structmatcher_test\.SimpleStruct \| 0x0>: nil\nto equal\n    <\*structmatcher_test\.SimpleStruct \| 0x[0-9a-f]+>: \{Field1: 7, Field2: ""}`))
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

	Describe("Opaque structures", func() {
		// unexported fields can't be accessed with reflect.Value.Interface()
		// Instead, if a (nested) struct contains any unexported field, we give
		// up on recursing, and use gomega.Equal() to do the comparison.
		// TODO: We only call Interface() b/c we use gomega.Equal() to do each
		//   field comparison. If we use some other way of comparing that
		//   doesn't require interface{} types, we could avoid this oddity.
		//   Unfortunately, reflect package doesn't expose deepValueEqual() that
		//   uses reflect.Value.
		type OpaqueStruct struct {
			privateField       string
			privateStructField SimpleStruct
		}
		type SemiOpaqueStruct struct {
			PublicField        SimpleStruct
			privateField       string
			privateStructField SimpleStruct
			PublicField2       SimpleStruct
		}
		type NestedOpaqueStruct struct {
			OpaqueField OpaqueStruct
			NormalField string
		}
		It("sees equal opaque fields as equal", func() {
			struct1 := NestedOpaqueStruct{
				OpaqueField: OpaqueStruct{
					privateField: "you can't see me!",
				},
				NormalField: "Hello",
			}
			struct2 := struct1
			Expect(struct2).To(structmatcher.MatchStruct(struct1))
		})
		It("compares unequal opaque fields with gomega.Equal()", func() {
			struct1 := NestedOpaqueStruct{
				OpaqueField: OpaqueStruct{
					privateField: "you can't see me!",
				},
				NormalField: "Hello",
			}
			struct2 := NestedOpaqueStruct{
				OpaqueField: OpaqueStruct{
					privateField: "you can't see me either!",
				},
				NormalField: "Hello",
			}
			messages := InterceptGomegaFailures(func() {
				Expect(struct2).To(structmatcher.MatchStruct(struct1))
			})
			Expect(messages).To(HaveLen(1))
			Expect(messages[0]).To(Equal("Expected structs to match but:\n" +
				"Mismatch on unexported field within OpaqueField\n" +
				"Expected\n" +
				"    <structmatcher_test.OpaqueStruct>: {\n" +
				"        privateField: \"you can't see me either!\",\n" +
				"        privateStructField: {Field1: 0, Field2: \"\"},\n" +
				"    }\n" +
				"to equal\n" +
				"    <structmatcher_test.OpaqueStruct>: {\n" +
				"        privateField: \"you can't see me!\",\n" +
				"        privateStructField: {Field1: 0, Field2: \"\"},\n" +
				"    }"))
		})
		It("still works when the top structs are opaque", func() {
			struct1 := OpaqueStruct{privateField: "foo"}
			struct2 := OpaqueStruct{privateField: "bar"}
			messages := InterceptGomegaFailures(func() {
				Expect(struct2).To(structmatcher.MatchStruct(struct1))
			})
			Expect(messages).To(HaveLen(1))
			Expect(messages[0]).To(Equal("Expected structs to match but:\n" +
				"Mismatch on unexported field within top level struct\n" +
				"Expected\n" +
				"    <structmatcher_test.OpaqueStruct>: {\n" +
				"        privateField: \"bar\",\n" +
				"        privateStructField: {Field1: 0, Field2: \"\"},\n" +
				"    }\n" +
				"to equal\n" +
				"    <structmatcher_test.OpaqueStruct>: {\n" +
				"        privateField: \"foo\",\n" +
				"        privateStructField: {Field1: 0, Field2: \"\"},\n" +
				"    }"))
		})
		It("works when public fields are also unequal", func() {
			struct1 := SemiOpaqueStruct{
				PublicField:  SimpleStruct{Field1: 1, Field2: "2"},
				privateField: "foo",
				PublicField2: SimpleStruct{Field1: 2, Field2: "2"},
			}
			struct2 := SemiOpaqueStruct{
				PublicField:  SimpleStruct{Field1: 10, Field2: "2"},
				privateField: "bar",
				PublicField2: SimpleStruct{Field1: 20, Field2: "2"},
			}
			messages := InterceptGomegaFailures(func() {
				Expect(struct2).To(structmatcher.MatchStruct(struct1))
			})
			Expect(messages).To(HaveLen(1))
			Expect(messages[0]).To(Equal("Expected structs to match but:\n" +
				"Mismatch on field PublicField.Field1\n" +
				"Expected\n" +
				"    <int>: 10\n" +
				"to equal\n" +
				"    <int>: 1\n" +
				"Mismatch on field PublicField2.Field1\n" +
				"Expected\n" +
				"    <int>: 20\n" +
				"to equal\n" +
				"    <int>: 2\n" +
				"Mismatch on unexported field within top level struct\n" +
				"Expected\n" +
				"    <structmatcher_test.SemiOpaqueStruct>: {\n" +
				"        PublicField: {Field1: 10, Field2: \"2\"},\n" +
				"        privateField: \"bar\",\n" +
				"        privateStructField: {Field1: 0, Field2: \"\"},\n" +
				"        PublicField2: {Field1: 20, Field2: \"2\"},\n" +
				"    }\n" +
				"to equal\n" +
				"    <structmatcher_test.SemiOpaqueStruct>: {\n" +
				"        PublicField: {Field1: 1, Field2: \"2\"},\n" +
				"        privateField: \"foo\",\n" +
				"        privateStructField: {Field1: 0, Field2: \"\"},\n" +
				"        PublicField2: {Field1: 2, Field2: \"2\"},\n" +
				"    }"))
		})
	})
})
