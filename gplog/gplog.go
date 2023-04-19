package gplog

/*
 * This file contains structs and functions related to logging.
 */

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"

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
	/*
	 * A mutex for ensuring that concurrent calls to output functions by multiple
	 * goroutines are safe.
	 *
	 * This mutex is a package-level global rather than a member of GpLogger to
	 * avoid any possible error condition caused by calling SetLogger in one
	 * goroutine while another is calling an output function, which could possibly
	 * arise while running tests in parallel.
	 */
	logMutex sync.Mutex
	/*
	 * A function which can customize log file name
	 */
	logFileNameFunc LogFileNameFunc
	exitFunc        ExitFunc
)

const (
	/*
	 * The following constants representing the current logging level, and are
	 * cumulative (that is, setting the log level to Debug will print all Error-,
	 * Info-, and Verbose-level messages in addition to Debug-level messages).
	 *
	 * Log levels for terminal output and logfile output are separate, and can be
	 * set independently.  By default, a new logger will have a verbosity of INFO
	 * for terminal output and DEBUG for logfile output.
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
 * - FatalWithoutPanic: Same as Fatal, but will not trigger panic. Just exit(1).
 */
type LogPrefixFunc func(string) string
type LogFileNameFunc func(string, string) string
type ExitFunc func()

type GpLogger struct {
	logStdout      *log.Logger
	logStderr      *log.Logger
	logFile        *log.Logger
	logFileName    string
	shellVerbosity int
	fileVerbosity  int
	header         string
	logPrefixFunc  LogPrefixFunc
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

	logfile := GenerateLogFileName(program, logdir)
	logFileHandle := openLogFile(logfile)

	logger = NewLogger(os.Stdout, os.Stderr, logFileHandle, logfile, LOGINFO, program)
	SetExitFunc(defaultExit)
}

func GenerateLogFileName(program, logdir string) string {
	var logfile string
	if logFileNameFunc != nil {
		logfile = logFileNameFunc(program, logdir)
	} else {
		timestamp := operating.System.Now().Format("20060102")
		logfile = fmt.Sprintf("%s/%s_%s.log", logdir, program, timestamp)
	}
	return logfile
}

func SetLogger(log *GpLogger) {
	logger = log
}

// This function should only be used for testing purposes
func GetLogger() *GpLogger {
	return logger
}

// stdout and stderr are passed in to this function to enable output redirection in tests.
func NewLogger(stdout io.Writer, stderr io.Writer, logFile io.Writer, logFileName string, shellVerbosity int, program string, logFileVerbosity ...int) *GpLogger {
	fileVerbosity := LOGDEBUG
	// Shell verbosity must always be specified, but file verbosity defaults to LOGDEBUG to encourage more verbose log output.
	if len(logFileVerbosity) == 1 && logFileVerbosity[0] >= LOGERROR && logFileVerbosity[0] <= LOGDEBUG {
		fileVerbosity = logFileVerbosity[0]
	}
	return &GpLogger{
		logStdout:      log.New(stdout, "", 0),
		logStderr:      log.New(stderr, "", 0),
		logFile:        log.New(logFile, "", 0),
		logFileName:    logFileName,
		shellVerbosity: shellVerbosity,
		fileVerbosity:  fileVerbosity,
		header:         GetHeader(program),
		logPrefixFunc:  nil,
	}
}

func GetHeader(program string) string {
	headerFormatStr := "%s:%s:%s:%06d-[%s]:-" // PROGRAMNAME:USERNAME:HOSTNAME:PID-[LOGLEVEL]:-
	currentUser, _ := operating.System.CurrentUser()
	user := currentUser.Username
	host, _ := operating.System.Hostname()
	pid := operating.System.Getpid()
	header := fmt.Sprintf(headerFormatStr, program, user, host, pid, "%s")
	return header
}

func SetLogPrefixFunc(logPrefixFunc func(string) string) {
	logger.logPrefixFunc = logPrefixFunc
}

func SetLogFileNameFunc(fileNameFunc func(string, string) string) {
	logFileNameFunc = fileNameFunc
}

func SetExitFunc(pExitFunc func()) {
	exitFunc = pExitFunc
}

func defaultLogPrefixFunc(level string) string {
	logTimestamp := operating.System.Now().Format("20060102:15:04:05")
	return fmt.Sprintf("%s %s", logTimestamp, fmt.Sprintf(logger.header, level))
}

func GetLogPrefix(level string) string {
	if logger.logPrefixFunc != nil {
		return logger.logPrefixFunc(level)
	}
	return defaultLogPrefixFunc(level)
}

func GetLogFilePath() string {
	return logger.logFileName
}

func GetVerbosity() int {
	return logger.shellVerbosity
}

func SetVerbosity(verbosity int) {
	logger.shellVerbosity = verbosity
}

func GetLogFileVerbosity() int {
	return logger.fileVerbosity
}

func SetLogFileVerbosity(verbosity int) {
	logger.fileVerbosity = verbosity
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
	logMutex.Lock()
	defer logMutex.Unlock()
	if logger.fileVerbosity >= LOGINFO || logger.shellVerbosity >= LOGINFO {
		message := GetLogPrefix("INFO") + fmt.Sprintf(s, v...)
		if logger.fileVerbosity >= LOGINFO {
			_ = logger.logFile.Output(1, message)
		}
		if logger.shellVerbosity >= LOGINFO {
			_ = logger.logStdout.Output(1, message)
		}
	}
}

func Warn(s string, v ...interface{}) {
	logMutex.Lock()
	defer logMutex.Unlock()
	message := GetLogPrefix("WARNING") + fmt.Sprintf(s, v...)
	_ = logger.logFile.Output(1, message)
	_ = logger.logStdout.Output(1, message)
}

func Verbose(s string, v ...interface{}) {
	logMutex.Lock()
	defer logMutex.Unlock()
	if logger.fileVerbosity >= LOGVERBOSE || logger.shellVerbosity >= LOGVERBOSE {
		message := GetLogPrefix("DEBUG") + fmt.Sprintf(s, v...)
		if logger.fileVerbosity >= LOGVERBOSE {
			_ = logger.logFile.Output(1, message)
		}
		if logger.shellVerbosity >= LOGVERBOSE {
			_ = logger.logStdout.Output(1, message)
		}
	}
}

func Debug(s string, v ...interface{}) {
	logMutex.Lock()
	defer logMutex.Unlock()
	if logger.fileVerbosity >= LOGDEBUG || logger.shellVerbosity >= LOGDEBUG {
		message := GetLogPrefix("DEBUG") + fmt.Sprintf(s, v...)
		if logger.fileVerbosity >= LOGDEBUG {
			_ = logger.logFile.Output(1, message)
		}
		if logger.shellVerbosity >= LOGDEBUG {
			_ = logger.logStdout.Output(1, message)
		}
	}
}

func Error(s string, v ...interface{}) {
	logMutex.Lock()
	defer logMutex.Unlock()
	message := GetLogPrefix("ERROR") + fmt.Sprintf(s, v...)
	errorCode = 1
	_ = logger.logFile.Output(1, message)
	_ = logger.logStderr.Output(1, message)
}

func Fatal(err error, s string, v ...interface{}) {
	logMutex.Lock()
	defer logMutex.Unlock()
	message := GetLogPrefix("CRITICAL")
	errorCode = 2
	stackTraceStr := ""
	if err != nil {
		message += fmt.Sprintf("%v", err)
		stackTraceStr = formatStackTrace(errors.WithStack(err))
		if s != "" {
			message += ": "
		}
	}
	message += strings.TrimSpace(fmt.Sprintf(s, v...))
	_ = logger.logFile.Output(1, message+stackTraceStr)
	if logger.shellVerbosity >= LOGVERBOSE {
		abort(message + stackTraceStr)
	} else {
		abort(message)
	}
}

func FatalOnError(err error, output ...string) {
	if err != nil {
		if len(output) == 0 {
			Fatal(err, "")
		} else {
			Fatal(err, output[0])
		}
	}
}

func FatalWithoutPanic(s string, v ...interface{}) {
	logMutex.Lock()
	defer logMutex.Unlock()
	message := GetLogPrefix("CRITICAL") + fmt.Sprintf(s, v...)
	errorCode = 2
	_ = logger.logFile.Output(1, message)
	_ = logger.logStderr.Output(1, message)
	exitFunc()
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}

func formatStackTrace(err error) string {
	st := err.(stackTracer).StackTrace()
	message := fmt.Sprintf("%+v", st[1:])
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

func defaultExit() {
	os.Exit(1)
}
