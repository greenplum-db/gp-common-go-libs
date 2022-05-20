package dbconn_test

import (
	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("dbconn/version tests", func() {
	fake43 := dbconn.GPDBVersion{VersionString: "4.3.0.0", SemVer: semver.MustParse("4.3.0")}
	fake50 := dbconn.GPDBVersion{VersionString: "5.0.0", SemVer: semver.MustParse("5.0.0")}
	fake51 := dbconn.GPDBVersion{VersionString: "5.1.0", SemVer: semver.MustParse("5.1.0")}
	Describe("StringToSemVerRange", func() {
		v400 := semver.MustParse("4.0.0")
		v500 := semver.MustParse("5.0.0")
		v510 := semver.MustParse("5.1.0")
		v501 := semver.MustParse("5.0.1")
		It(`turns "=5" into a range matching 5.x`, func() {
			resultRange := dbconn.StringToSemVerRange("=5")
			Expect(resultRange(v400)).To(BeFalse())
			Expect(resultRange(v500)).To(BeTrue())
			Expect(resultRange(v510)).To(BeTrue())
			Expect(resultRange(v501)).To(BeTrue())
		})
		It(`turns "=5.0" into a range matching 5.0.x`, func() {
			resultRange := dbconn.StringToSemVerRange("=5.0")
			Expect(resultRange(v400)).To(BeFalse())
			Expect(resultRange(v500)).To(BeTrue())
			Expect(resultRange(v510)).To(BeFalse())
			Expect(resultRange(v501)).To(BeTrue())
		})
		It(`turns "=5.0.0" into a range matching 5.0.0`, func() {
			resultRange := dbconn.StringToSemVerRange("=5.0.0")
			Expect(resultRange(v400)).To(BeFalse())
			Expect(resultRange(v500)).To(BeTrue())
			Expect(resultRange(v510)).To(BeFalse())
			Expect(resultRange(v501)).To(BeFalse())
		})
	})
	Describe("Before", func() {
		It("returns true when comparing 4.3 to 5", func() {
			connection.Version = fake43
			result := connection.Version.Before("5")
			Expect(result).To(BeTrue())
		})
		It("returns true when comparing 5 to 5.1", func() {
			connection.Version = fake50
			result := connection.Version.Before("5.1")
			Expect(result).To(BeTrue())
		})
		It("returns false when comparing 5 to 5", func() {
			connection.Version = fake50
			result := connection.Version.Before("5")
			Expect(result).To(BeFalse())
		})
	})
	Describe("AtLeast", func() {
		It("returns true when comparing 5 to 4.3", func() {
			connection.Version = fake50
			result := connection.Version.AtLeast("4")
			Expect(result).To(BeTrue())
		})
		It("returns true when comparing 5 to 5", func() {
			connection.Version = fake50
			result := connection.Version.AtLeast("5")
			Expect(result).To(BeTrue())
		})
		It("returns true when comparing 5.1 to 5.0", func() {
			connection.Version = fake51
			result := connection.Version.AtLeast("5")
			Expect(result).To(BeTrue())
		})
		It("returns false when comparing 4.3 to 5", func() {
			connection.Version = fake43
			result := connection.Version.AtLeast("5")
			Expect(result).To(BeFalse())
		})
		It("returns false when comparing 5.0 to 5.1", func() {
			connection.Version = fake50
			result := connection.Version.AtLeast("5.1")
			Expect(result).To(BeFalse())
		})
	})
	Describe("Is", func() {
		It("returns true when comparing 5 to 5", func() {
			connection.Version = fake50
			result := connection.Version.Is("5")
			Expect(result).To(BeTrue())
		})
		It("returns true when comparing 5.1 to 5", func() {
			connection.Version = fake51
			result := connection.Version.Is("5")
			Expect(result).To(BeTrue())
		})
		It("returns false when comparing 5.0 to 5.1", func() {
			connection.Version = fake50
			result := connection.Version.Is("5.1")
			Expect(result).To(BeFalse())
		})
		It("returns false when comparing 4.3 to 5", func() {
			connection.Version = fake43
			result := connection.Version.Is("5")
			Expect(result).To(BeFalse())
		})
	})
})
