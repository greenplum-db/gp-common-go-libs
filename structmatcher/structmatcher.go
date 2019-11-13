package structmatcher

/*
 * This file contains test structs and functions used in unit tests via dependency injection.
 */

import (
	"reflect"
	"strings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
)

/*
 * If fields are to be filtered in or out, set shouldFilter to true; filterInclude is true to
 * include fields or false to exclude fields, and filterFields contains the field names to filter on.
 * To filter on a field "fieldname" in struct "structname", pass in "fieldname".
 * To filter on a field "fieldname" in a nested struct under field "structfield", pass in "structfield.fieldname".
 * This function assumes structs will only ever be nested one level deep.
 */
func StructMatcher(expected interface{}, actual interface{}, shouldFilter bool, filterInclude bool, filterFields ...string) []string {
	// Add field names for the top-level struct to a filter map, and split off nested field names to pass down to nested structs
	filterMap := make(map[string]bool)
	nestedFilterFields := make([]string, 0)
	for i := 0; i < len(filterFields); i++ {
		fieldNames := strings.Split(filterFields[i], ".")
		if len(fieldNames) == 2 {
			nestedFilterFields = append(nestedFilterFields, fieldNames[1])
			// If we include a nested struct field, we also need to include the nested struct
			if filterInclude {
				filterMap[fieldNames[0]] = true
			}
		} else {
			filterMap[filterFields[i]] = true
		}
	}
	expectedStruct := reflect.Indirect(reflect.ValueOf(expected))
	actualStruct := reflect.Indirect(reflect.ValueOf(actual))
	mismatches := []string{}
	mismatches = append(mismatches, InterceptGomegaFailures(func() {
		for i := 0; i < expectedStruct.NumField(); i++ {
			expectedField := expectedStruct.Field(i)
			actualField := actualStruct.Field(i)
			fieldName := actualStruct.Type().Field(i).Name
			// If we're including, skip this field if the name doesn't match; if we're excluding, skip if it does match
			if shouldFilter && ((filterInclude && !filterMap[fieldName]) || (!filterInclude && filterMap[fieldName])) {
				continue
			}
			actualFieldIsNonemptySlice := actualField.Kind() == reflect.Slice && !actualField.IsNil() && actualField.Len() > 0
			expectedFieldIsNonemptySlice := expectedField.Kind() == reflect.Slice && !expectedField.IsNil() && expectedField.Len() > 0
			fieldIsStructSlice := actualFieldIsNonemptySlice && expectedFieldIsNonemptySlice && actualField.Len() == expectedField.Len() && actualField.Index(0).Kind() == reflect.Struct
			if fieldIsStructSlice {
				for j := 0; j < actualField.Len(); j++ {
					expectedStructField := expectedStruct.Field(i).Index(j).Interface()
					actualStructField := actualStruct.Field(i).Index(j).Interface()
					mismatches = append(mismatches, StructMatcher(expectedStructField, actualStructField, shouldFilter, filterInclude, nestedFilterFields...)...)
				}
			} else {
				expectedValue := expectedStruct.Field(i).Interface()
				actualValue := actualStruct.Field(i).Interface()
				Expect(actualValue).To(Equal(expectedValue), "Mismatch on field %s", fieldName)
			}
		}
	})...)
	return mismatches
}

// Deprecated: Use structmatcher.MatchStruct() GomegaMatcher
func ExpectStructsToMatch(expected interface{}, actual interface{}) {
	mismatches := StructMatcher(expected, actual, false, false)
	if len(mismatches) > 0 {
		Fail(strings.Join(mismatches, "\n"))
	}
}

// Deprecated: Use structmatcher.MatchStruct().ExcludingFields() GomegaMatcher
func ExpectStructsToMatchExcluding(expected interface{}, actual interface{}, excludeFields ...string) {
	mismatches := StructMatcher(expected, actual, true, false, excludeFields...)
	if len(mismatches) > 0 {
		Fail(strings.Join(mismatches, "\n"))
	}
}

// Deprecated: Use structmatcher.MatchStruct().IncludingFields() GomegaMatcher
func ExpectStructsToMatchIncluding(expected interface{}, actual interface{}, includeFields ...string) {
	mismatches := StructMatcher(expected, actual, true, true, includeFields...)
	if len(mismatches) > 0 {
		Fail(strings.Join(mismatches, "\n"))
	}
}

type Matcher struct {
	expected        interface{}
	includingFields []string
	excludingFields []string
	mismatches      []string
}

var _ types.GomegaMatcher = &Matcher{}

func MatchStruct(expected interface{}) *Matcher {
	return &Matcher{
		expected: expected,
	}
}

func (m *Matcher) Match(actual interface{}) (success bool, err error) {
	if m.includingFields != nil {
		m.mismatches = StructMatcher(m.expected, actual, true, true, m.includingFields...)
	} else if m.excludingFields != nil {
		m.mismatches = StructMatcher(m.expected, actual, true, false, m.excludingFields...)
	} else {
		m.mismatches = StructMatcher(m.expected, actual, false, false)
	}
	return len(m.mismatches) == 0, nil
}

func (m *Matcher) FailureMessage(actual interface{}) (message string) {
	//return format.Message(actual, "to match struct", m.expected)
	return "Expected structs to match but:\n"+strings.Join(m.mismatches, "\n")
}

func (m *Matcher) NegatedFailureMessage(actual interface{}) (message string) {
	return "Expected structs not to match, but they did"
}

func (m *Matcher) IncludingFields(fields ...string) *Matcher {
	m.includingFields = fields
	return m
}

func (m *Matcher) ExcludingFields(fields ...string) *Matcher {
	m.excludingFields = fields
	return m
}
