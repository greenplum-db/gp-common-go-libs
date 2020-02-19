package dbconn

import (
	"regexp"
	"strings"

	"github.com/blang/semver"
)

type GPDBVersion struct {
	VersionString string
	SemVer        semver.Version
}

/*
 * This constructor is intended as a convenience function for testing and
 * setting defaults; the dbconn.Connect function will automatically initialize
 * the version of the database to which it is connecting.
 *
 * The versionStr argument here should be a semantic version in the form X.Y.Z,
 * not a GPDB version string like the one returned by "SELECT version()".  If
 * an invalid semantic version is passed, that is considered programmer error
 * and the function will panic.
 */
func NewVersion(versionStr string) GPDBVersion {
	version := GPDBVersion{
		VersionString: versionStr,
		SemVer:        semver.MustParse(versionStr),
	}
	return version
}

func InitializeVersion(dbconn *DBConn) (dbversion GPDBVersion, err error) {
	err = dbconn.Get(&dbversion, "SELECT pg_catalog.version() AS versionstring")
	if err != nil {
		return
	}
	versionStart := strings.Index(dbversion.VersionString, "(Greenplum Database ") + len("(Greenplum Database ")
	versionEnd := strings.Index(dbversion.VersionString, ")")
	dbversion.VersionString = dbversion.VersionString[versionStart:versionEnd]

	pattern := regexp.MustCompile(`\d+\.\d+\.\d+`)
	threeDigitVersion := pattern.FindStringSubmatch(dbversion.VersionString)[0]
	dbversion.SemVer, err = semver.Make(threeDigitVersion)
	return
}

func StringToSemVerRange(versionStr string) semver.Range {
	numDigits := len(strings.Split(versionStr, "."))
	if numDigits < 3 {
		versionStr += ".x"
	}
	validRange := semver.MustParseRange(versionStr)
	return validRange
}

func (dbversion GPDBVersion) Before(targetVersion string) bool {
	validRange := StringToSemVerRange("<" + targetVersion)
	return validRange(dbversion.SemVer)
}

func (dbversion GPDBVersion) AtLeast(targetVersion string) bool {
	validRange := StringToSemVerRange(">=" + targetVersion)
	return validRange(dbversion.SemVer)
}

func (dbversion GPDBVersion) Is(targetVersion string) bool {
	validRange := StringToSemVerRange("==" + targetVersion)
	return validRange(dbversion.SemVer)
}
