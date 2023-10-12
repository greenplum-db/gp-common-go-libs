// Package ui will provide hooks into STDOUT, STDERR and STDIN. It will also
// handle translation as necessary.
// This has been pilfered from CF CLI https://github.com/cloudfoundry/cli/blob/master/util/ui/ui.go
package gpvui

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"

	"gitlab.eng.vmware.com/tanzu-data-services/greenplum/gp-virtual/gp-virtual-operator/pkg/constants"
	"gitlab.eng.vmware.com/tanzu-data-services/greenplum/gp-virtual/gp-virtual-operator/pkg/gpverror"
	"gitlab.eng.vmware.com/tanzu-data-services/greenplum/gp-virtual/gp-virtual-operator/pkg/gpvlog"
	"golang.org/x/term"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

//counterfeiter:generate . IoReader
type IoReader interface {
	ReadString(delim byte) (string, error)
}

var (
	gpvUi *GpvUiImpl
)

func init() {
	Default()
}

var Dependency struct {
	DisplayTextToStream      func(isErrStream bool, text string)
	GetGpvLogger             func() gpvlog.GpvLogger
	GetGpvUi                 func() GpvUi
	GetWarningFormat         func(statusCode constants.WarningCode) string
	NewGpvError              func(errorCode constants.ErrorCode, args ...any) gpverror.Error
	NewReader                func(rd io.Reader) IoReader
	ReadPasswordFromTerminal func(fd int) ([]byte, error)
}

func Default() {
	Dependency.DisplayTextToStream = DisplayTextToStream
	Dependency.GetGpvLogger = gpvlog.GetGpvLogger
	Dependency.GetGpvUi = GetGpvUi
	Dependency.GetWarningFormat = gpverror.GetWarningFormat
	Dependency.NewGpvError = gpverror.New
	Dependency.NewReader = newReader
	Dependency.ReadPasswordFromTerminal = term.ReadPassword

	gpvUi = &GpvUiImpl{}

	gpvUi.SetOut(os.Stdout)
	gpvUi.SetErr(os.Stderr)
	gpvUi.SetIn(os.Stdin)
}

//counterfeiter:generate . GpvUi
type GpvUi interface {
	DisplayError(format string, args ...any)
	DisplayErrorNoNewline(format string, args ...any)
	DisplayPrompt(format string, args ...any)
	DisplayText(format string, args ...any)
	DisplayTextNoNewline(format string, args ...any)
	DisplayWarning(warningCode constants.WarningCode, args ...any)
	GetErr() io.Writer
	GetErrLogWriter() io.Writer
	GetIn() io.Reader
	GetOut() io.Writer
	GetOutLogWriter() io.Writer
	ReadInput(prompt string) (string, error)
	ReadPassword(prompt string) (string, error)
	SetErr(e io.Writer)
	SetIn(i io.Reader)
	SetOut(o io.Writer)
}

type GpvUiImpl struct {
	Err    io.Writer
	In     io.Reader
	Out    io.Writer
	Reader IoReader
}

func GetGpvUi() GpvUi {
	return gpvUi
}

func (g *GpvUiImpl) SetOut(out io.Writer) {
	g.Out = out
}

func (g *GpvUiImpl) GetOut() io.Writer {
	return g.Out
}

func (g *GpvUiImpl) SetErr(err io.Writer) {
	g.Err = err
}

func (g *GpvUiImpl) GetErr() io.Writer {
	return g.Err
}

func (g *GpvUiImpl) SetIn(in io.Reader) {
	g.In = in
	g.Reader = Dependency.NewReader(in)
}

func (g *GpvUiImpl) GetIn() io.Reader {
	return g.In
}

// DisplayError prints the error message to ui.Err, with a trailing '\n', and to the log.
func (g *GpvUiImpl) DisplayError(format string, args ...any) {
	g.DisplayErrorNoNewline(format, args...)
	fmt.Fprintf(g.Err, "\n")
}

// DisplayErrorNoNewline prints the error message to ui.Err, without a trailing '\n', and to the log.
func (g *GpvUiImpl) DisplayErrorNoNewline(format string, args ...any) {
	fmt.Fprintf(g.Err, format, args...)

	text := fmt.Sprintf(format, args...)
	lines := strings.Split(text, "\n")
	for _, line := range lines {
		Dependency.GetGpvLogger().Error(line)
	}
}

// DisplayPrompt prints the message to ui.Out, with a trailing ' ', and to the log.
func (g *GpvUiImpl) DisplayPrompt(format string, args ...any) {
	g.DisplayTextNoNewline(format, args...)
	fmt.Fprintf(g.Out, " ")
}

// DisplayText prints the message to ui.Out, with a trailing '\n', and to the log.
func (g *GpvUiImpl) DisplayText(format string, args ...any) {
	g.DisplayTextNoNewline(format, args...)
	fmt.Fprintf(g.Out, "\n")
}

// DisplayTextNoNewline prints the message to ui.Out, without a trailing '\n', and to the log.
func (g *GpvUiImpl) DisplayTextNoNewline(format string, args ...any) {
	fmt.Fprintf(g.Out, format, args...)

	text := fmt.Sprintf(format, args...)
	lines := strings.Split(text, "\n")
	for _, line := range lines {
		Dependency.GetGpvLogger().Info(line)
	}
}

// DisplayWarning prints a warning to ui.Stdout, with a trailing '\n', and to the log.
func (g *GpvUiImpl) DisplayWarning(warningCode constants.WarningCode, args ...any) {
	format := fmt.Sprintf("WARN [%04d] %s", warningCode, Dependency.GetWarningFormat(warningCode))
	fmt.Fprintf(g.Out, format, args...)
	fmt.Fprintf(g.Out, "\n")
	text := fmt.Sprintf(format, args...)
	Dependency.GetGpvLogger().Warn(text)
}

func (g *GpvUiImpl) GetErrLogWriter() io.Writer {
	return &GpvStreamLogWriterImpl{IsErrStream: true}
}

func (g *GpvUiImpl) GetOutLogWriter() io.Writer {
	return &GpvStreamLogWriterImpl{}
}

func (g *GpvUiImpl) ReadInput(prompt string) (string, error) {
	g.DisplayPrompt(prompt)

	result, err := g.Reader.ReadString('\n')
	if err != nil {
		return "", Dependency.NewGpvError(gpverror.FailedToGetUserInput, err)
	}

	return strings.Trim(result, "\n"), nil
}

func (g *GpvUiImpl) ReadPassword(prompt string) (string, error) {
	g.DisplayPrompt(prompt)

	bytePassword, err := Dependency.ReadPasswordFromTerminal(syscall.Stdin)
	if err != nil {
		return "", Dependency.NewGpvError(gpverror.FailedToReadPassword, err)
	}

	// Carriage return after the user input
	g.DisplayText("")

	password := string(bytePassword)

	return password, nil
}

type GpvStreamLogWriterImpl struct {
	IsErrStream bool
}

func (w *GpvStreamLogWriterImpl) Write(p []byte) (int, error) {
	size := len(p)
	if size > 0 {
		Dependency.DisplayTextToStream(w.IsErrStream, string(p))
	}

	return size, nil
}

func DisplayTextToStream(isErrStream bool, text string) {
	reformatted := strings.ReplaceAll(text, "%", "%%")
	if isErrStream {
		Dependency.GetGpvUi().DisplayErrorNoNewline(reformatted)
	} else {
		Dependency.GetGpvUi().DisplayTextNoNewline(reformatted)
	}
}

func newReader(rd io.Reader) IoReader {
	return bufio.NewReader(rd)
}
