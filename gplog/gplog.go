package gplog

/*
 * This file contains structs and functions related to logging.
 */

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/user"
	"time"

	"github.com/pkg/errors"
)

var (
	/*
	 * Error code key:
	 *   0: Completed successfully (default value)
	 *   1: Completed, but encountered a non-fatal error (set by logger.Error)
	 *   2: Did not complete, encountered a fatal error (set by logger.Fatal)
	 */
	errorCode              = 0
	HeaderFuncs HeaderInfo = &RealHeaderInfo{}
)

const (
	/*
	 * The following constants representing the current logging level, and are
	 * cumulative (that is, setting the log level to Debug will print all Error-,
	 * Info-, and Verbose-level messages in addition to Debug-level messages).
	 */
	LOGERROR = iota
	LOGINFO
	LOGVERBOSE
	LOGDEBUG
)

/*
 * Leveled logging output functions using the above log levels are implemented
 * below.  Info(), Verbose(), and Debug() print messages when the log level is
 * set at or above the log level matching their names.  Warn(), Error(), and
 * Fatal() always print their messages regardless of the current log level.
 *
 * The intended usage of these functions is as follows:
 * - Info: Messages that should always be written unless the user explicitly
 *         suppresses output, e.g. the timestamp that will be used in the backup.
 * - Verbose: More detailed messages that are mostly useful to the user, e.g.
 *            printing information about a function's substeps for progress tracking.
 * - Debug: More detailed messages that are mostly useful to developers, e.g.
 *          noting that a function has been called with certain arguments.
 * - Warn: Messages indicating unusual but not incorrect behavior that a user
 *         may want to know, e.g. that certain steps are skipped when using
 *         certain flags.  These messages are shown even if output is suppressed.
 * - Error: Messages indicating that an error has occurred, but that the program
 *          can continue, e.g. one table failed to back up but others succeeded.
 * - Fatal: Messages indicating that the program cannot proceed, e.g. the database
 *          cannot be reached.  This function will exit the program after printing
 *          the error message.
 */

type Logger struct {
	logStdout   *log.Logger
	logStderr   *log.Logger
	logFile     *log.Logger
	logFileName string
	verbosity   int
	header      string
	HeaderInfo
}

type HeaderInfo interface {
	CurrentUser() (*user.User, error)
	Getpid() int
	Hostname() (string, error)
	Now() time.Time
}

type RealHeaderInfo struct{}

func (headerInfo *RealHeaderInfo) CurrentUser() (*user.User, error) {
	return user.Current()
}

func (headerInfo *RealHeaderInfo) Getpid() int {
	return os.Getpid()
}

func (headerInfo *RealHeaderInfo) Hostname() (string, error) {
	return os.Hostname()
}

func (headerInfo *RealHeaderInfo) Now() time.Time {
	return time.Now()
}

/*
 * Logger initialization/helper functions
 */

// stdout and stderr are passed in to this function to enable output redirection in tests.
func NewLogger(stdout io.Writer, stderr io.Writer, logFile io.Writer, logFileName string, verbosity int, program string) *Logger {
	return &Logger{
		logStdout:   log.New(stdout, "", 0),
		logStderr:   log.New(stderr, "", 0),
		logFile:     log.New(logFile, "", 0),
		logFileName: logFileName,
		verbosity:   verbosity,
		header:      getHeader(program),
	}
}

func getHeader(program string) string {
	headerFormatStr := "%s:%s:%s:%06d-[%s]:-" // PROGRAMNAME:USERNAME:HOSTNAME:PID-[LOGLEVEL]:-
	currentUser, _ := HeaderFuncs.CurrentUser()
	user := currentUser.Username
	host, _ := HeaderFuncs.Hostname()
	pid := HeaderFuncs.Getpid()
	header := fmt.Sprintf(headerFormatStr, program, user, host, pid, "%s")
	return header

}

func (logger *Logger) GetLogPrefix(level string) string {
	logTimestamp := HeaderFuncs.Now().Format("20060102:15:04:05")
	return fmt.Sprintf("%s %s", logTimestamp, fmt.Sprintf(logger.header, level))
}

func (logger *Logger) GetLogFilePath() string {
	return logger.logFileName
}

func (logger *Logger) GetVerbosity() int {
	return logger.verbosity
}

func (logger *Logger) SetVerbosity(verbosity int) {
	logger.verbosity = verbosity
}

/*
 * Log output functions, as described above
 */

func (logger *Logger) Info(s string, v ...interface{}) {
	message := logger.GetLogPrefix("INFO") + fmt.Sprintf(s, v...)
	logger.logFile.Output(1, message)
	if logger.verbosity >= LOGINFO {
		logger.logStdout.Output(1, message)
	}
}

func (logger *Logger) Warn(s string, v ...interface{}) {
	message := logger.GetLogPrefix("WARNING") + fmt.Sprintf(s, v...)
	logger.logFile.Output(1, message)
	logger.logStdout.Output(1, message)
}

func (logger *Logger) Verbose(s string, v ...interface{}) {
	message := logger.GetLogPrefix("DEBUG") + fmt.Sprintf(s, v...)
	logger.logFile.Output(1, message)
	if logger.verbosity >= LOGVERBOSE {
		logger.logStdout.Output(1, message)
	}
}

func (logger *Logger) Debug(s string, v ...interface{}) {
	message := logger.GetLogPrefix("DEBUG") + fmt.Sprintf(s, v...)
	logger.logFile.Output(1, message)
	if logger.verbosity >= LOGDEBUG {
		logger.logStdout.Output(1, message)
	}
}

func (logger *Logger) Error(s string, v ...interface{}) {
	message := logger.GetLogPrefix("ERROR") + fmt.Sprintf(s, v...)
	errorCode = 1
	logger.logFile.Output(1, message)
	logger.logStderr.Output(1, message)
}

func (logger *Logger) Fatal(err error, s string, v ...interface{}) {
	message := logger.GetLogPrefix("CRITICAL") + fmt.Sprintf(s, v...)
	errorCode = 2
	stackTraceStr := ""
	if err != nil {
		if s != "" {
			message += ": "
		}
		message += fmt.Sprintf("%v", err)
		stackTraceStr = formatStackTrace(errors.WithStack(err))
	}
	logger.logFile.Output(1, message+stackTraceStr)
	if logger.verbosity >= LOGVERBOSE {
		abort(message + stackTraceStr)
	} else {
		abort(message)
	}
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}

func formatStackTrace(err error) string {
	st := err.(stackTracer).StackTrace()
	message := fmt.Sprintf("%+v", st[1:len(st)-2])
	return message
}

func GetErrorCode() int {
	return errorCode
}

/*
 * Abort() is for handling critical errors.  It panic()s to unwind the call stack
 * until the panic is caught by the recover() in DoTeardown() in backup.go, at
 * which point any necessary cleanup is performed.
 *
 * log.Fatal() calls Abort() after logging its arguments, so generally that function
 * should be used instead of calling Abort() directly.
 */

func abort(output ...interface{}) {
	errStr := ""
	if len(output) > 0 {
		errStr = fmt.Sprintf("%v", output[0])
		if len(output) > 1 {
			errStr = fmt.Sprintf(errStr, output[1:]...)
		}
	}
	panic(errStr)
}
