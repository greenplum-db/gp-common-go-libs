package gplog

/*
 * This file contains structs and functions related to logging.
 */

import (
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
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

// --- constants for colorized output support
const escape = "\x1b"

const (
	NONE = iota
	RED
	GREEN
	YELLOW
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
	logStdout          *log.Logger
	logStderr          *log.Logger
	logFile            *log.Logger
	logFileName        string
	shellVerbosity     int
	fileVerbosity      int
	header             string
	shellHeader        string
	fileHeader         string
	logPrefixFunc      LogPrefixFunc
	shellLogPrefixFunc LogPrefixFunc
	colorize           bool
}

// levelsToPrefix is a regex for determining if the message level will be shown on console
// by the DefaultShortLogPrefixFunc function
var levelsToPrefix = regexp.MustCompile(`WARNING|ERROR|CRITICAL`)

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
		logStdout:          log.New(stdout, "", 0),
		logStderr:          log.New(stderr, "", 0),
		logFile:            log.New(logFile, "", 0),
		logFileName:        logFileName,
		shellVerbosity:     shellVerbosity,
		fileVerbosity:      fileVerbosity,
		header:             GetHeader(program),
		logPrefixFunc:      nil,
		shellLogPrefixFunc: nil,
		colorize:           false,
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

// SetShellLogPrefixFunc registers a function that returns a prefix for messages that get printed to the shell console
func SetShellLogPrefixFunc(logPrefixFunc func(string) string) {
	logger.shellLogPrefixFunc = logPrefixFunc
}

// SetColorize sets the flag defining whether to colorize the output to the shell console.
// The output to log files is never colorized, even if the flag is set to true.
// Shell console output colorization depends on the message levels:
// red      - for CRITICAL (non-panic) and ERROR levels
// yellow   - for WARNING levels
// green    - for INFO levels produced via Success function call only
// no color - for all other levels
func SetColorize(shouldColorize bool) {
	logger.colorize = shouldColorize
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

// DefaultShortLogPrefixFunc returns a short prefix for messages typically sent to the shell console
// It does not include the timestamp and other system information that would be found in a log file
// and only displays the message logging level for WARNING, ERROR, and CRITICAL levels. This function must be
// explicitly set by the users of the logger by calling SetShellLogPrefixFunc(gplog.DefaultShortLogPrefixFunc)
func DefaultShortLogPrefixFunc(level string) string {
	if levelsToPrefix.MatchString(level) {
		return fmt.Sprintf("%s: ", level)
	}
	return ""
}

func GetLogPrefix(level string) string {
	if logger.logPrefixFunc != nil {
		return logger.logPrefixFunc(level)
	}
	return defaultLogPrefixFunc(level)
}

// GetShellLogPrefix returns a prefix to prepend to the message before sending it to the shell console
// Use this mechanism if it is desired to have different prefixes for messages sent to the console and to the log file.
// A caller must first set a custom function that will provide a custom prefix by calling SetShellLogPrefixFunc.
// If the custom function has not been provided, this function returns a prefix produced by the GelLogPrefix function,
// so that the prefixes for the shell console and the log file will be the same.
func GetShellLogPrefix(level string) string {
	if logger.shellLogPrefixFunc != nil {
		return logger.shellLogPrefixFunc(level)
	}
	return GetLogPrefix(level)
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
	if logger.fileVerbosity >= LOGINFO {
		message := GetLogPrefix("INFO") + fmt.Sprintf(s, v...)
		_ = logger.logFile.Output(1, message)
	}
	if logger.shellVerbosity >= LOGINFO {
		message := GetShellLogPrefix("INFO") + fmt.Sprintf(s, v...)
		_ = logger.logStdout.Output(1, message)
	}
}

func Success(s string, v ...interface{}) {
	logMutex.Lock()
	defer logMutex.Unlock()
	if logger.fileVerbosity >= LOGINFO {
		message := GetLogPrefix("INFO") + fmt.Sprintf(s, v...)
		_ = logger.logFile.Output(1, message)
	}
	if logger.shellVerbosity >= LOGINFO {
		message := GetShellLogPrefix("INFO") + fmt.Sprintf(s, v...)
		_ = logger.logStdout.Output(1, Colorize(GREEN, message))
	}
}

func Warn(s string, v ...interface{}) {
	logMutex.Lock()
	defer logMutex.Unlock()
	message := GetLogPrefix("WARNING") + fmt.Sprintf(s, v...)
	_ = logger.logFile.Output(1, message)
	message = GetShellLogPrefix("WARNING") + fmt.Sprintf(s, v...)
	_ = logger.logStdout.Output(1, Colorize(YELLOW, message))
}

/*
 * Progress is for messages that show progress as an alternative to a progress bar.
 * We write them to the log file if fileVerbosity is >= LOGINFO, and we write them to stdout if shellVerbosity >= LOGVERBOSE
 */

func Progress(s string, v ...interface{}) {
	logMutex.Lock()
	defer logMutex.Unlock()
	var message string
	if logger.fileVerbosity >= LOGINFO {
		message = GetLogPrefix("INFO") + fmt.Sprintf(s, v...)
		_ = logger.logFile.Output(1, message)
	}
	if logger.shellVerbosity >= LOGVERBOSE {
		message = GetShellLogPrefix("DEBUG") + fmt.Sprintf(s, v...)
		_ = logger.logStdout.Output(1, message)
	}
}

func Verbose(s string, v ...interface{}) {
	logMutex.Lock()
	defer logMutex.Unlock()
	if logger.fileVerbosity >= LOGVERBOSE {
		message := GetLogPrefix("DEBUG") + fmt.Sprintf(s, v...)
		_ = logger.logFile.Output(1, message)
	}
	if logger.shellVerbosity >= LOGVERBOSE {
		message := GetShellLogPrefix("DEBUG") + fmt.Sprintf(s, v...)
		_ = logger.logStdout.Output(1, message)
	}
}

func Debug(s string, v ...interface{}) {
	logMutex.Lock()
	defer logMutex.Unlock()
	if logger.fileVerbosity >= LOGDEBUG {
		message := GetLogPrefix("DEBUG") + fmt.Sprintf(s, v...)
		_ = logger.logFile.Output(1, message)
	}
	if logger.shellVerbosity >= LOGDEBUG {
		message := GetShellLogPrefix("DEBUG") + fmt.Sprintf(s, v...)
		_ = logger.logStdout.Output(1, message)
	}
}

func Error(s string, v ...interface{}) {
	logMutex.Lock()
	defer logMutex.Unlock()
	errorCode = 1
	message := GetLogPrefix("ERROR") + fmt.Sprintf(s, v...)
	_ = logger.logFile.Output(1, message)
	message = GetShellLogPrefix("ERROR") + fmt.Sprintf(s, v...)
	_ = logger.logStderr.Output(1, Colorize(RED, message))
}

func Fatal(err error, s string, v ...interface{}) {
	logMutex.Lock()
	defer logMutex.Unlock()
	errorCode = 2
	suffix := ""
	stackTraceStr := ""
	if err != nil {
		suffix += fmt.Sprintf("%v", err)
		stackTraceStr = formatStackTrace(errors.WithStack(err))
		if s != "" {
			suffix += ": "
		}
	}
	suffix += strings.TrimSpace(fmt.Sprintf(s, v...))
	message := GetLogPrefix("CRITICAL") + suffix
	_ = logger.logFile.Output(1, message+stackTraceStr)
	message = GetShellLogPrefix("CRITICAL") + suffix
	// messages for panic are not colorized to allow any recover logic to inspect the actual message
	// if the message needs to be output to the shell console, the caller should colorize it explicitly, if desired
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
	errorCode = 2
	message := GetLogPrefix("CRITICAL") + fmt.Sprintf(s, v...)
	_ = logger.logFile.Output(1, message)
	message = GetShellLogPrefix("CRITICAL") + fmt.Sprintf(s, v...)
	_ = logger.logStderr.Output(1, Colorize(RED, message))
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

// color returns special characters that should be prepended to a string to make it of a particular color on the console
func color(c int) string {
	if c == NONE {
		return fmt.Sprintf("%s[%dm", escape, c)
	}
	return fmt.Sprintf("%s[3%dm", escape, c)
}

// Colorize wraps a string with special characters so that the string has a provided color when output to the console
// colorization happens only if the logger flag `colorize` is set to true. The function is exported to allow
// colorization outside the logging methods, such as when recovering from a `panic` when Fatal messages are logged.
func Colorize(c int, text string) string {
	if logger.colorize {
		return color(c) + text + color(NONE)
	}
	return text
}
