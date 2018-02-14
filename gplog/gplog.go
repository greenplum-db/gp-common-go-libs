package gplog

/*
 * This file contains structs and functions related to logging.
 */

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/pkg/errors"
)

var (
	/*
	 * Error code key:
	 *   0: Completed successfully (default value)
	 *   1: Completed, but encountered a non-fatal error (set by logger.Error)
	 *   2: Did not complete, encountered a fatal error (set by logger.Fatal)
	 */
	errorCode = 0
	// Singleton logger used by any package or utility that calls InitializeLogging
	logger *GpLogger
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
 *         suppresses output, e.g. notifying the user that a step has completed.
 * - Verbose: More detailed messages that are mostly useful to the user, e.g.
 *            printing information about a function's substeps for progress tracking.
 * - Debug: More detailed messages that are mostly useful to developers, e.g.
 *          noting that a function has been called with certain arguments.
 * - Warn: Messages indicating unusual but not incorrect behavior that a user
 *         may want to know, e.g. that certain steps are skipped when using
 *         certain flags.  These messages are shown even if output is suppressed.
 * - Error: Messages indicating that an error has occurred, but that the program
 *          can continue, e.g. one function call in a group failed but others succeeded.
 * - Fatal: Messages indicating that the program cannot proceed, e.g. the database
 *          cannot be reached.  This function will exit the program after printing
 *          the error message.
 */

type GpLogger struct {
	logStdout   *log.Logger
	logStderr   *log.Logger
	logFile     *log.Logger
	logFileName string
	verbosity   int
	header      string
}

/*
 * Logger initialization/helper functions
 */

/*
 * Multiple calls to InitializeLogging can be made if desired; the first call
 * will initialize the logger as a singleton and subsequent calls will return
 * the same Logger instance.
 */
func InitializeLogging(program string, logdir string) {
	if logger != nil {
		return
	}
	currentUser, _ := operating.System.CurrentUser()
	if logdir == "" {
		logdir = fmt.Sprintf("%s/gpAdminLogs", currentUser.HomeDir)
	}

	createLogDirectory(logdir)
	timestamp := operating.System.Now().Format("20060102")
	logfile := fmt.Sprintf("%s/%s_%s.log", logdir, program, timestamp)
	logFileHandle := openLogFile(logfile)

	logger = NewLogger(os.Stdout, os.Stderr, logFileHandle, logfile, LOGINFO, program)
}

func SetLogger(log *GpLogger) {
	logger = log
}

// This function should only be used for testing purposes
func GetLogger() *GpLogger {
	return logger
}

// stdout and stderr are passed in to this function to enable output redirection in tests.
func NewLogger(stdout io.Writer, stderr io.Writer, logFile io.Writer, logFileName string, verbosity int, program string) *GpLogger {
	return &GpLogger{
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
	currentUser, _ := operating.System.CurrentUser()
	user := currentUser.Username
	host, _ := operating.System.Hostname()
	pid := operating.System.Getpid()
	header := fmt.Sprintf(headerFormatStr, program, user, host, pid, "%s")
	return header

}

func GetLogPrefix(level string) string {
	logTimestamp := operating.System.Now().Format("20060102:15:04:05")
	return fmt.Sprintf("%s %s", logTimestamp, fmt.Sprintf(logger.header, level))
}

func GetLogFilePath() string {
	return logger.logFileName
}

func GetVerbosity() int {
	return logger.verbosity
}

func SetVerbosity(verbosity int) {
	logger.verbosity = verbosity
}

func GetErrorCode() int {
	return errorCode
}

func SetErrorCode(code int) {
	errorCode = code
}

/*
 * Log output functions, as described above
 */

func Info(s string, v ...interface{}) {
	message := GetLogPrefix("INFO") + fmt.Sprintf(s, v...)
	logger.logFile.Output(1, message)
	if logger.verbosity >= LOGINFO {
		logger.logStdout.Output(1, message)
	}
}

func Warn(s string, v ...interface{}) {
	message := GetLogPrefix("WARNING") + fmt.Sprintf(s, v...)
	logger.logFile.Output(1, message)
	logger.logStdout.Output(1, message)
}

func Verbose(s string, v ...interface{}) {
	message := GetLogPrefix("DEBUG") + fmt.Sprintf(s, v...)
	logger.logFile.Output(1, message)
	if logger.verbosity >= LOGVERBOSE {
		logger.logStdout.Output(1, message)
	}
}

func Debug(s string, v ...interface{}) {
	message := GetLogPrefix("DEBUG") + fmt.Sprintf(s, v...)
	logger.logFile.Output(1, message)
	if logger.verbosity >= LOGDEBUG {
		logger.logStdout.Output(1, message)
	}
}

func Error(s string, v ...interface{}) {
	message := GetLogPrefix("ERROR") + fmt.Sprintf(s, v...)
	errorCode = 1
	logger.logFile.Output(1, message)
	logger.logStderr.Output(1, message)
}

func Fatal(err error, s string, v ...interface{}) {
	message := GetLogPrefix("CRITICAL") + fmt.Sprintf(s, v...)
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

func FatalOnError(err error) {
	if err != nil {
		Fatal(err, "")
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

/*
 * Abort() is for handling critical errors.  It panic()s to unwind the call stack
 * assuming that the panic is caught by a recover() in the main utility.
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
func openLogFile(filename string) io.WriteCloser {
	flags := os.O_APPEND | os.O_CREATE | os.O_WRONLY
	fileHandle, err := operating.System.OpenFileWrite(filename, flags, 0644)
	if err != nil {
		abort(err)
	}
	return fileHandle
}

func createLogDirectory(dirname string) {
	info, err := operating.System.Stat(dirname)
	if err != nil {
		if operating.System.IsNotExist(err) {
			err = operating.System.MkdirAll(dirname, 0755)
			if err != nil {
				abort(errors.Errorf("Cannot create log directory %s: %v", dirname, err))
			}
		} else {
			abort(errors.Errorf("Cannot stat log directory %s: %v", dirname, err))
		}
	} else if !(info.IsDir()) {
		abort(errors.Errorf("%s is a file, not a directory", dirname))
	}
}
