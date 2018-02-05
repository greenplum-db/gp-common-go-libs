package gplog_test

import (
	"fmt"
	"io"
	"os"
	"os/user"
	"reflect"
	"testing"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/pkg/errors"
)

func TestGpLog(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "gplog tests")
}

var _ = Describe("logger/log tests", func() {
	var (
		logger       *gplog.Logger
		stdout       *gbytes.Buffer
		stderr       *gbytes.Buffer
		logfile      *gbytes.Buffer
		buffer       *gbytes.Buffer
		sampleLogger *gplog.Logger
		fakeInfo     os.FileInfo
	)

	BeforeEach(func() {
		err := os.MkdirAll("/tmp/log_dir", 0755)
		Expect(err).ToNot(HaveOccurred())
		fakeInfo, err = os.Stat("/tmp/log_dir")
		Expect(err).ToNot(HaveOccurred())

		operating.System.CurrentUser = func() (*user.User, error) { return &user.User{Username: "testUser", HomeDir: "testDir"}, nil }
		operating.System.Getpid = func() int { return 0 }
		operating.System.Hostname = func() (string, error) { return "testHost", nil }
		operating.System.IsNotExist = func(err error) bool { return false }
		operating.System.Now = func() time.Time { return time.Date(2017, time.January, 1, 1, 1, 1, 1, time.Local) }
		operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) { return buffer, nil }
		operating.System.Stat = func(name string) (os.FileInfo, error) { return fakeInfo, nil }
		logger, stdout, stderr, logfile = testhelper.SetupTestLogger()
	})
	AfterEach(func() {
		operating.System = operating.InitializeSystemFunctions()
	})
	Describe("InitializeLogging", func() {
		BeforeEach(func() {
			sampleLogger = gplog.NewLogger(os.Stdout, os.Stderr, buffer, "testDir/gpAdminLogs/testProgram_20170101.log",
				gplog.LOGINFO, "testProgram")
			gplog.SetLogger(nil)
		})
		Context("Logger initialized with default log directory and Info log level", func() {
			It("creates a new logger writing to gpAdminLogs and sets utils.logger to this new logger", func() {
				newLogger := gplog.InitializeLogging("testProgram", "")
				if !reflect.DeepEqual(newLogger, sampleLogger) {
					Fail(fmt.Sprintf("Created logger does not match sample logger:\n%v\n%v", newLogger, sampleLogger))
				}
			})
		})
		Context("Logger initialized with a specified log directory and Info log level", func() {
			It("creates a new logger writing to the specified log directory and sets utils.logger to this new logger", func() {
				sampleLogger = gplog.NewLogger(os.Stdout, os.Stderr, buffer, "/tmp/log_dir/testProgram_20170101.log",
					gplog.LOGINFO, "testProgram")
				newLogger := gplog.InitializeLogging("testProgram", "/tmp/log_dir")
				if !reflect.DeepEqual(newLogger, sampleLogger) {
					Fail(fmt.Sprintf("Created logger does not match sample logger:\n%v\n%v", newLogger, sampleLogger))
				}
			})
		})
		Context("Directory or log file does not exist or is not writable", func() {
			It("creates a log directory if given a nonexistent log directory", func() {
				calledWith := ""
				operating.System.IsNotExist = func(err error) bool { return true }
				operating.System.Stat = func(name string) (os.FileInfo, error) {
					calledWith = name
					return fakeInfo, errors.New("file does not exist")
				}
				gplog.InitializeLogging("testProgram", "/tmp/log_dir")
				Expect(calledWith).To(Equal("/tmp/log_dir"))
			})
			It("creates a log file if given a nonexistent log file", func() {
				calledWith := ""
				operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
					calledWith = name
					return buffer, nil
				}
				operating.System.IsNotExist = func(err error) bool { return true }
				operating.System.Stat = func(name string) (os.FileInfo, error) { return fakeInfo, errors.New("file does not exist") }
				gplog.InitializeLogging("testProgram", "/tmp/log_dir")
				Expect(calledWith).To(Equal("/tmp/log_dir/testProgram_20170101.log"))
			})
			It("panics if given a non-writable log directory", func() {
				operating.System.Stat = func(name string) (os.FileInfo, error) { return fakeInfo, errors.New("permission denied") }
				defer testhelper.ShouldPanicWithMessage("permission denied")
				gplog.InitializeLogging("testProgram", "/tmp/log_dir")
			})
			It("panics if given a non-writable log file", func() {
				operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
					return nil, errors.New("permission denied")
				}
				defer testhelper.ShouldPanicWithMessage("permission denied")
				gplog.InitializeLogging("testProgram", "/tmp/log_dir")
			})
		})
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
					testhelper.NotExpectRegexp(stdout, infoExpected+expectedMessage)
					testhelper.NotExpectRegexp(stderr, infoExpected+expectedMessage)
					testhelper.ExpectRegexp(logfile, infoExpected+expectedMessage)
				})
			})
			Context("Warn", func() {
				It("prints to stdout and the log file", func() {
					expectedMessage := "error warn"
					logger.Warn(expectedMessage)
					testhelper.ExpectRegexp(stdout, warnExpected+expectedMessage)
					testhelper.NotExpectRegexp(stderr, warnExpected+expectedMessage)
					testhelper.ExpectRegexp(logfile, warnExpected+expectedMessage)
				})
			})
			Context("Verbose", func() {
				It("prints to the log file", func() {
					expectedMessage := "error verbose"
					logger.Verbose(expectedMessage)
					testhelper.NotExpectRegexp(stdout, verboseExpected+expectedMessage)
					testhelper.NotExpectRegexp(stderr, verboseExpected+expectedMessage)
					testhelper.ExpectRegexp(logfile, verboseExpected+expectedMessage)
				})
			})
			Context("Debug", func() {
				It("prints to the log file", func() {
					expectedMessage := "error debug"
					logger.Debug(expectedMessage)
					testhelper.NotExpectRegexp(stdout, debugExpected+expectedMessage)
					testhelper.NotExpectRegexp(stderr, debugExpected+expectedMessage)
					testhelper.ExpectRegexp(logfile, debugExpected+expectedMessage)
				})
			})
			Context("Error", func() {
				It("prints to stderr and the log file", func() {
					expectedMessage := "error error"
					logger.Error(expectedMessage)
					testhelper.NotExpectRegexp(stdout, errorExpected+expectedMessage)
					testhelper.ExpectRegexp(stderr, errorExpected+expectedMessage)
					testhelper.ExpectRegexp(logfile, errorExpected+expectedMessage)
				})
			})
			Context("Fatal", func() {
				It("prints to the log file, then panics", func() {
					expectedMessage := "error fatal"
					defer func() {
						testhelper.NotExpectRegexp(stdout, fatalExpected+expectedMessage)
						testhelper.NotExpectRegexp(stderr, fatalExpected+expectedMessage)
						testhelper.ExpectRegexp(logfile, fatalExpected+expectedMessage)
					}()
					defer testhelper.ShouldPanicWithMessage(expectedMessage)
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
					testhelper.ExpectRegexp(stdout, infoExpected+expectedMessage)
					testhelper.NotExpectRegexp(stderr, infoExpected+expectedMessage)
					testhelper.ExpectRegexp(logfile, infoExpected+expectedMessage)
				})
			})
			Context("Warn", func() {
				It("prints to stdout and the log file", func() {
					expectedMessage := "info warn"
					logger.Warn(expectedMessage)
					testhelper.ExpectRegexp(stdout, warnExpected+expectedMessage)
					testhelper.NotExpectRegexp(stderr, warnExpected+expectedMessage)
					testhelper.ExpectRegexp(logfile, warnExpected+expectedMessage)
				})
			})
			Context("Verbose", func() {
				It("prints to the log file", func() {
					expectedMessage := "info verbose"
					logger.Verbose(expectedMessage)
					testhelper.NotExpectRegexp(stdout, verboseExpected+expectedMessage)
					testhelper.NotExpectRegexp(stderr, verboseExpected+expectedMessage)
					testhelper.ExpectRegexp(logfile, verboseExpected+expectedMessage)
				})
			})
			Context("Debug", func() {
				It("prints to the log file", func() {
					expectedMessage := "info debug"
					logger.Debug(expectedMessage)
					testhelper.NotExpectRegexp(stdout, debugExpected+expectedMessage)
					testhelper.NotExpectRegexp(stderr, debugExpected+expectedMessage)
					testhelper.ExpectRegexp(logfile, debugExpected+expectedMessage)
				})
			})
			Context("Error", func() {
				It("prints to stderr and the log file", func() {
					expectedMessage := "info error"
					logger.Error(expectedMessage)
					testhelper.NotExpectRegexp(stdout, errorExpected+expectedMessage)
					testhelper.ExpectRegexp(stderr, errorExpected+expectedMessage)
					testhelper.ExpectRegexp(logfile, errorExpected+expectedMessage)
				})
			})
			Context("Fatal", func() {
				It("prints to the log file, then panics", func() {
					expectedMessage := "info fatal"
					defer func() {
						testhelper.NotExpectRegexp(stdout, fatalExpected+expectedMessage)
						testhelper.NotExpectRegexp(stderr, fatalExpected+expectedMessage)
						testhelper.ExpectRegexp(logfile, fatalExpected+expectedMessage)
					}()
					defer testhelper.ShouldPanicWithMessage(expectedMessage)
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
					testhelper.ExpectRegexp(stdout, infoExpected+expectedMessage)
					testhelper.NotExpectRegexp(stderr, infoExpected+expectedMessage)
					testhelper.ExpectRegexp(logfile, infoExpected+expectedMessage)
				})
			})
			Context("Warn", func() {
				It("prints to stdout and the log file", func() {
					expectedMessage := "verbose warn"
					logger.Warn(expectedMessage)
					testhelper.ExpectRegexp(stdout, warnExpected+expectedMessage)
					testhelper.NotExpectRegexp(stderr, warnExpected+expectedMessage)
					testhelper.ExpectRegexp(logfile, warnExpected+expectedMessage)
				})
			})
			Context("Verbose", func() {
				It("prints to stdout and the log file", func() {
					expectedMessage := "verbose verbose"
					logger.Verbose(expectedMessage)
					testhelper.ExpectRegexp(stdout, verboseExpected+expectedMessage)
					testhelper.NotExpectRegexp(stderr, verboseExpected+expectedMessage)
					testhelper.ExpectRegexp(logfile, verboseExpected+expectedMessage)
				})
			})
			Context("Debug", func() {
				It("prints to the log file", func() {
					expectedMessage := "verbose debug"
					logger.Debug(expectedMessage)
					testhelper.NotExpectRegexp(stdout, debugExpected+expectedMessage)
					testhelper.NotExpectRegexp(stderr, debugExpected+expectedMessage)
					testhelper.ExpectRegexp(logfile, debugExpected+expectedMessage)
				})
			})
			Context("Error", func() {
				It("prints to stderr and the log file", func() {
					expectedMessage := "verbose error"
					logger.Error(expectedMessage)
					testhelper.NotExpectRegexp(stdout, errorExpected+expectedMessage)
					testhelper.ExpectRegexp(stderr, errorExpected+expectedMessage)
					testhelper.ExpectRegexp(logfile, errorExpected+expectedMessage)
				})
			})
			Context("Fatal", func() {
				It("prints to the log file, then panics", func() {
					expectedMessage := "verbose fatal"
					defer func() {
						testhelper.NotExpectRegexp(stdout, fatalExpected+expectedMessage)
						testhelper.NotExpectRegexp(stderr, fatalExpected+expectedMessage)
						testhelper.ExpectRegexp(logfile, fatalExpected+expectedMessage)
					}()
					defer testhelper.ShouldPanicWithMessage(expectedMessage)
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
					testhelper.ExpectRegexp(stdout, infoExpected+expectedMessage)
					testhelper.NotExpectRegexp(stderr, infoExpected+expectedMessage)
					testhelper.ExpectRegexp(logfile, infoExpected+expectedMessage)
				})
			})
			Context("Warn", func() {
				It("prints to stdout and the log file", func() {
					expectedMessage := "debug warn"
					logger.Warn(expectedMessage)
					testhelper.ExpectRegexp(stdout, warnExpected+expectedMessage)
					testhelper.NotExpectRegexp(stderr, warnExpected+expectedMessage)
					testhelper.ExpectRegexp(logfile, warnExpected+expectedMessage)
				})
			})
			Context("Verbose", func() {
				It("prints to stdout and the log file", func() {
					expectedMessage := "debug verbose"
					logger.Verbose(expectedMessage)
					testhelper.ExpectRegexp(stdout, verboseExpected+expectedMessage)
					testhelper.NotExpectRegexp(stderr, verboseExpected+expectedMessage)
					testhelper.ExpectRegexp(logfile, verboseExpected+expectedMessage)
				})
			})
			Context("Debug", func() {
				It("prints to stdout and the log file", func() {
					expectedMessage := "debug debug"
					logger.Debug(expectedMessage)
					testhelper.ExpectRegexp(stdout, debugExpected+expectedMessage)
					testhelper.NotExpectRegexp(stderr, debugExpected+expectedMessage)
					testhelper.ExpectRegexp(logfile, debugExpected+expectedMessage)
				})
			})
			Context("Error", func() {
				It("prints to stderr and the log file", func() {
					expectedMessage := "debug error"
					logger.Error(expectedMessage)
					testhelper.NotExpectRegexp(stdout, errorExpected+expectedMessage)
					testhelper.ExpectRegexp(stderr, errorExpected+expectedMessage)
					testhelper.ExpectRegexp(logfile, errorExpected+expectedMessage)
				})
			})
			Context("Fatal", func() {
				It("prints to the log file, then panics", func() {
					expectedMessage := "debug fatal"
					defer func() {
						testhelper.NotExpectRegexp(stdout, fatalExpected+expectedMessage)
						testhelper.NotExpectRegexp(stderr, fatalExpected+expectedMessage)
						testhelper.ExpectRegexp(logfile, fatalExpected+expectedMessage)
					}()
					defer testhelper.ShouldPanicWithMessage(expectedMessage)
					logger.Fatal(errors.New(expectedMessage), "")
				})
			})
		})
	})
})
