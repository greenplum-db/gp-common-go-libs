package gplog_test

import (
	"fmt"
	"os/user"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/pkg/errors"
)

func TestGpLog(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "gplog tests")
}

// Test helper functions
func SetupTestLogger() (*gplog.Logger, *gbytes.Buffer, *gbytes.Buffer, *gbytes.Buffer) {
	testStdout := gbytes.NewBuffer()
	testStderr := gbytes.NewBuffer()
	testLogfile := gbytes.NewBuffer()
	testLogger := gplog.NewLogger(testStdout, testStderr, testLogfile, "gbytes.Buffer", gplog.LOGINFO, "testProgram")
	return testLogger, testStdout, testStderr, testLogfile
}

type TestHeaderInfo struct{}

func (headerInfo *TestHeaderInfo) CurrentUser() (*user.User, error) {
	return &user.User{Username: "testUser", HomeDir: "testDir"}, nil
}

func (headerInfo *TestHeaderInfo) Getpid() int {
	return 0
}

func (headerInfo *TestHeaderInfo) Hostname() (string, error) {
	return "testHost", nil
}

func (headerInfo *TestHeaderInfo) Now() time.Time {
	return time.Date(2017, time.January, 1, 1, 1, 1, 1, time.Local)
}
func ExpectRegexp(buffer *gbytes.Buffer, testStr string) {
	Expect(buffer).Should(gbytes.Say(regexp.QuoteMeta(testStr)))
}

func NotExpectRegexp(buffer *gbytes.Buffer, testStr string) {
	Expect(buffer).ShouldNot(gbytes.Say(regexp.QuoteMeta(testStr)))
}

func ShouldPanicWithMessage(message string) {
	if r := recover(); r != nil {
		errorMessage := strings.TrimSpace(fmt.Sprintf("%v", r))
		if !strings.Contains(errorMessage, message) {
			Fail(fmt.Sprintf("Expected panic message '%s', got '%s'", message, errorMessage))
		}
	} else {
		Fail("Function did not panic as expected")
	}
}

var _ = Describe("logger/log tests", func() {
	var (
		logger  *gplog.Logger
		stdout  *gbytes.Buffer
		stderr  *gbytes.Buffer
		logfile *gbytes.Buffer
	)

	BeforeEach(func() {
		gplog.HeaderFuncs = &TestHeaderInfo{}
		logger, stdout, stderr, logfile = SetupTestLogger()
	})
	Describe("GetLogPrefix", func() {
		It("returns a prefix for the current time", func() {
			expectedMessage := "20170101:01:01:01 testProgram:testUser:testHost:000000-[INFO]:-"
			prefix := logger.GetLogPrefix("INFO")
			Expect(expectedMessage).To(Equal(prefix))
		})
	})
	Describe("Output function tests", func() {
		patternExpected := "20170101:01:01:01 testProgram:testUser:testHost:000000-[%s]:-"
		infoExpected := fmt.Sprintf(patternExpected, "INFO")
		warnExpected := fmt.Sprintf(patternExpected, "WARNING")
		verboseExpected := fmt.Sprintf(patternExpected, "DEBUG")
		debugExpected := fmt.Sprintf(patternExpected, "DEBUG")
		errorExpected := fmt.Sprintf(patternExpected, "ERROR")
		fatalExpected := fmt.Sprintf(patternExpected, "CRITICAL")

		Describe("Verbosity set to Error", func() {
			BeforeEach(func() {
				logger.SetVerbosity(gplog.LOGERROR)
			})

			Context("Info", func() {
				It("prints to the log file", func() {
					expectedMessage := "error info"
					logger.Info(expectedMessage)
					NotExpectRegexp(stdout, infoExpected+expectedMessage)
					NotExpectRegexp(stderr, infoExpected+expectedMessage)
					ExpectRegexp(logfile, infoExpected+expectedMessage)
				})
			})
			Context("Warn", func() {
				It("prints to stdout and the log file", func() {
					expectedMessage := "error warn"
					logger.Warn(expectedMessage)
					ExpectRegexp(stdout, warnExpected+expectedMessage)
					NotExpectRegexp(stderr, warnExpected+expectedMessage)
					ExpectRegexp(logfile, warnExpected+expectedMessage)
				})
			})
			Context("Verbose", func() {
				It("prints to the log file", func() {
					expectedMessage := "error verbose"
					logger.Verbose(expectedMessage)
					NotExpectRegexp(stdout, verboseExpected+expectedMessage)
					NotExpectRegexp(stderr, verboseExpected+expectedMessage)
					ExpectRegexp(logfile, verboseExpected+expectedMessage)
				})
			})
			Context("Debug", func() {
				It("prints to the log file", func() {
					expectedMessage := "error debug"
					logger.Debug(expectedMessage)
					NotExpectRegexp(stdout, debugExpected+expectedMessage)
					NotExpectRegexp(stderr, debugExpected+expectedMessage)
					ExpectRegexp(logfile, debugExpected+expectedMessage)
				})
			})
			Context("Error", func() {
				It("prints to stderr and the log file", func() {
					expectedMessage := "error error"
					logger.Error(expectedMessage)
					NotExpectRegexp(stdout, errorExpected+expectedMessage)
					ExpectRegexp(stderr, errorExpected+expectedMessage)
					ExpectRegexp(logfile, errorExpected+expectedMessage)
				})
			})
			Context("Fatal", func() {
				It("prints to the log file, then panics", func() {
					expectedMessage := "error fatal"
					defer func() {
						NotExpectRegexp(stdout, fatalExpected+expectedMessage)
						NotExpectRegexp(stderr, fatalExpected+expectedMessage)
						ExpectRegexp(logfile, fatalExpected+expectedMessage)
					}()
					defer ShouldPanicWithMessage(expectedMessage)
					logger.Fatal(errors.New(expectedMessage), "")
				})
			})
		})
		Describe("Verbosity set to Info", func() {
			BeforeEach(func() {
				logger.SetVerbosity(gplog.LOGINFO)
			})

			Context("Info", func() {
				It("prints to stdout and the log file", func() {
					expectedMessage := "info info"
					logger.Info(expectedMessage)
					ExpectRegexp(stdout, infoExpected+expectedMessage)
					NotExpectRegexp(stderr, infoExpected+expectedMessage)
					ExpectRegexp(logfile, infoExpected+expectedMessage)
				})
			})
			Context("Warn", func() {
				It("prints to stdout and the log file", func() {
					expectedMessage := "info warn"
					logger.Warn(expectedMessage)
					ExpectRegexp(stdout, warnExpected+expectedMessage)
					NotExpectRegexp(stderr, warnExpected+expectedMessage)
					ExpectRegexp(logfile, warnExpected+expectedMessage)
				})
			})
			Context("Verbose", func() {
				It("prints to the log file", func() {
					expectedMessage := "info verbose"
					logger.Verbose(expectedMessage)
					NotExpectRegexp(stdout, verboseExpected+expectedMessage)
					NotExpectRegexp(stderr, verboseExpected+expectedMessage)
					ExpectRegexp(logfile, verboseExpected+expectedMessage)
				})
			})
			Context("Debug", func() {
				It("prints to the log file", func() {
					expectedMessage := "info debug"
					logger.Debug(expectedMessage)
					NotExpectRegexp(stdout, debugExpected+expectedMessage)
					NotExpectRegexp(stderr, debugExpected+expectedMessage)
					ExpectRegexp(logfile, debugExpected+expectedMessage)
				})
			})
			Context("Error", func() {
				It("prints to stderr and the log file", func() {
					expectedMessage := "info error"
					logger.Error(expectedMessage)
					NotExpectRegexp(stdout, errorExpected+expectedMessage)
					ExpectRegexp(stderr, errorExpected+expectedMessage)
					ExpectRegexp(logfile, errorExpected+expectedMessage)
				})
			})
			Context("Fatal", func() {
				It("prints to the log file, then panics", func() {
					expectedMessage := "info fatal"
					defer func() {
						NotExpectRegexp(stdout, fatalExpected+expectedMessage)
						NotExpectRegexp(stderr, fatalExpected+expectedMessage)
						ExpectRegexp(logfile, fatalExpected+expectedMessage)
					}()
					defer ShouldPanicWithMessage(expectedMessage)
					logger.Fatal(errors.New(expectedMessage), "")
				})
			})
		})
		Describe("Verbosity set to Verbose", func() {
			BeforeEach(func() {
				logger.SetVerbosity(gplog.LOGVERBOSE)
			})

			Context("Info", func() {
				It("prints to stdout and the log file", func() {
					expectedMessage := "verbose info"
					logger.Info(expectedMessage)
					ExpectRegexp(stdout, infoExpected+expectedMessage)
					NotExpectRegexp(stderr, infoExpected+expectedMessage)
					ExpectRegexp(logfile, infoExpected+expectedMessage)
				})
			})
			Context("Warn", func() {
				It("prints to stdout and the log file", func() {
					expectedMessage := "verbose warn"
					logger.Warn(expectedMessage)
					ExpectRegexp(stdout, warnExpected+expectedMessage)
					NotExpectRegexp(stderr, warnExpected+expectedMessage)
					ExpectRegexp(logfile, warnExpected+expectedMessage)
				})
			})
			Context("Verbose", func() {
				It("prints to stdout and the log file", func() {
					expectedMessage := "verbose verbose"
					logger.Verbose(expectedMessage)
					ExpectRegexp(stdout, verboseExpected+expectedMessage)
					NotExpectRegexp(stderr, verboseExpected+expectedMessage)
					ExpectRegexp(logfile, verboseExpected+expectedMessage)
				})
			})
			Context("Debug", func() {
				It("prints to the log file", func() {
					expectedMessage := "verbose debug"
					logger.Debug(expectedMessage)
					NotExpectRegexp(stdout, debugExpected+expectedMessage)
					NotExpectRegexp(stderr, debugExpected+expectedMessage)
					ExpectRegexp(logfile, debugExpected+expectedMessage)
				})
			})
			Context("Error", func() {
				It("prints to stderr and the log file", func() {
					expectedMessage := "verbose error"
					logger.Error(expectedMessage)
					NotExpectRegexp(stdout, errorExpected+expectedMessage)
					ExpectRegexp(stderr, errorExpected+expectedMessage)
					ExpectRegexp(logfile, errorExpected+expectedMessage)
				})
			})
			Context("Fatal", func() {
				It("prints to the log file, then panics", func() {
					expectedMessage := "verbose fatal"
					defer func() {
						NotExpectRegexp(stdout, fatalExpected+expectedMessage)
						NotExpectRegexp(stderr, fatalExpected+expectedMessage)
						ExpectRegexp(logfile, fatalExpected+expectedMessage)
					}()
					defer ShouldPanicWithMessage(expectedMessage)
					logger.Fatal(errors.New(expectedMessage), "")
				})
			})
		})
		Describe("Verbosity set to Debug", func() {
			BeforeEach(func() {
				logger.SetVerbosity(gplog.LOGDEBUG)
			})

			Context("Info", func() {
				It("prints to stdout and the log file", func() {
					expectedMessage := "debug info"
					logger.Info(expectedMessage)
					ExpectRegexp(stdout, infoExpected+expectedMessage)
					NotExpectRegexp(stderr, infoExpected+expectedMessage)
					ExpectRegexp(logfile, infoExpected+expectedMessage)
				})
			})
			Context("Warn", func() {
				It("prints to stdout and the log file", func() {
					expectedMessage := "debug warn"
					logger.Warn(expectedMessage)
					ExpectRegexp(stdout, warnExpected+expectedMessage)
					NotExpectRegexp(stderr, warnExpected+expectedMessage)
					ExpectRegexp(logfile, warnExpected+expectedMessage)
				})
			})
			Context("Verbose", func() {
				It("prints to stdout and the log file", func() {
					expectedMessage := "debug verbose"
					logger.Verbose(expectedMessage)
					ExpectRegexp(stdout, verboseExpected+expectedMessage)
					NotExpectRegexp(stderr, verboseExpected+expectedMessage)
					ExpectRegexp(logfile, verboseExpected+expectedMessage)
				})
			})
			Context("Debug", func() {
				It("prints to stdout and the log file", func() {
					expectedMessage := "debug debug"
					logger.Debug(expectedMessage)
					ExpectRegexp(stdout, debugExpected+expectedMessage)
					NotExpectRegexp(stderr, debugExpected+expectedMessage)
					ExpectRegexp(logfile, debugExpected+expectedMessage)
				})
			})
			Context("Error", func() {
				It("prints to stderr and the log file", func() {
					expectedMessage := "debug error"
					logger.Error(expectedMessage)
					NotExpectRegexp(stdout, errorExpected+expectedMessage)
					ExpectRegexp(stderr, errorExpected+expectedMessage)
					ExpectRegexp(logfile, errorExpected+expectedMessage)
				})
			})
			Context("Fatal", func() {
				It("prints to the log file, then panics", func() {
					expectedMessage := "debug fatal"
					defer func() {
						NotExpectRegexp(stdout, fatalExpected+expectedMessage)
						NotExpectRegexp(stderr, fatalExpected+expectedMessage)
						ExpectRegexp(logfile, fatalExpected+expectedMessage)
					}()
					defer ShouldPanicWithMessage(expectedMessage)
					logger.Fatal(errors.New(expectedMessage), "")
				})
			})
		})
	})
})
