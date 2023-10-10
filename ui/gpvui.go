// Package ui will provide hooks into STDOUT, STDERR and STDIN. It will also
// handle translation as necessary.
// This has been pilfered from CF CLI https://github.com/cloudfoundry/cli/blob/master/util/ui/ui.go
package ui

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"

	"github.com/greenplum-db/gp-common-go-libs/constants"
	error "github.com/greenplum-db/gp-common-go-libs/error"
	"golang.org/x/term"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

//counterfeiter:generate . IoReader
type IoReader interface {
	ReadString(delim byte) (string, error)
}

var (
	ui *UiImpl
)

func init() {
	Default()
}

var Dependency struct {
	DisplayTextToStream      func(isErrStream bool, text string)
	Getlogger                func() log.logger
	GetUi                    func() ui
	GetWarningFormat         func(statusCode constants.WarningCode) string
	NewError                 func(errorCode constants.ErrorCode, args ...any) error.Error
	NewReader                func(rd io.Reader) IoReader
	ReadPasswordFromTerminal func(fd int) ([]byte, error)
}

func Default() {
	Dependency.DisplayTextToStream = DisplayTextToStream
	Dependency.Getlogger = log.Getlogger
	Dependency.GetUi = GetUi
	Dependency.GetWarningFormat = error.GetWarningFormat
	Dependency.NewError = error.New
	Dependency.NewReader = newReader
	Dependency.ReadPasswordFromTerminal = term.ReadPassword

	ui = &UiImpl{}

	ui.SetOut(os.Stdout)
	ui.SetErr(os.Stderr)
	ui.SetIn(os.Stdin)
}

//counterfeiter:generate . Ui
type Ui interface {
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

type UiImpl struct {
	Err    io.Writer
	In     io.Reader
	Out    io.Writer
	Reader IoReader
}

func GetUi() ui {
	return ui
}

func (g *UiImpl) SetOut(out io.Writer) {
	g.Out = out
}

func (g *UiImpl) GetOut() io.Writer {
	return g.Out
}

func (g *UiImpl) SetErr(err io.Writer) {
	g.Err = err
}

func (g *UiImpl) GetErr() io.Writer {
	return g.Err
}

func (g *UiImpl) SetIn(in io.Reader) {
	g.In = in
	g.Reader = Dependency.NewReader(in)
}

func (g *UiImpl) GetIn() io.Reader {
	return g.In
}

// DisplayError prints the error message to ui.Err, with a trailing '\n', and to the log.
func (g *UiImpl) DisplayError(format string, args ...any) {
	g.DisplayErrorNoNewline(format, args...)
	fmt.Fprintf(g.Err, "\n")
}

// DisplayErrorNoNewline prints the error message to ui.Err, without a trailing '\n', and to the log.
func (g *UiImpl) DisplayErrorNoNewline(format string, args ...any) {
	fmt.Fprintf(g.Err, format, args...)

	text := fmt.Sprintf(format, args...)
	lines := strings.Split(text, "\n")
	for _, line := range lines {
		Dependency.Getlogger().Error(line)
	}
}

// DisplayPrompt prints the message to ui.Out, with a trailing ' ', and to the log.
func (g *UiImpl) DisplayPrompt(format string, args ...any) {
	g.DisplayTextNoNewline(format, args...)
	fmt.Fprintf(g.Out, " ")
}

// DisplayText prints the message to ui.Out, with a trailing '\n', and to the log.
func (g *UiImpl) DisplayText(format string, args ...any) {
	g.DisplayTextNoNewline(format, args...)
	fmt.Fprintf(g.Out, "\n")
}

// DisplayTextNoNewline prints the message to ui.Out, without a trailing '\n', and to the log.
func (g *UiImpl) DisplayTextNoNewline(format string, args ...any) {
	fmt.Fprintf(g.Out, format, args...)

	text := fmt.Sprintf(format, args...)
	lines := strings.Split(text, "\n")
	for _, line := range lines {
		Dependency.Getlogger().Info(line)
	}
}

// DisplayWarning prints a warning to ui.Stdout, with a trailing '\n', and to the log.
func (g *UiImpl) DisplayWarning(warningCode constants.WarningCode, args ...any) {
	format := fmt.Sprintf("WARN [%04d] %s", warningCode, Dependency.GetWarningFormat(warningCode))
	fmt.Fprintf(g.Out, format, args...)
	fmt.Fprintf(g.Out, "\n")
	text := fmt.Sprintf(format, args...)
	Dependency.Getlogger().Warn(text)
}

func (g *UiImpl) GetErrLogWriter() io.Writer {
	return &GpvStreamLogWriterImpl{IsErrStream: true}
}

func (g *UiImpl) GetOutLogWriter() io.Writer {
	return &GpvStreamLogWriterImpl{}
}

func (g *UiImpl) ReadInput(prompt string) (string, error) {
	g.DisplayPrompt(prompt)

	result, err := g.Reader.ReadString('\n')
	if err != nil {
		return "", Dependency.NewError(error.FailedToGetUserInput, err)
	}

	return strings.Trim(result, "\n"), nil
}

func (g *UiImpl) ReadPassword(prompt string) (string, error) {
	g.DisplayPrompt(prompt)

	bytePassword, err := Dependency.ReadPasswordFromTerminal(syscall.Stdin)
	if err != nil {
		return "", Dependency.NewError(error.FailedToReadPassword, err)
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
		Dependency.GetUi().DisplayErrorNoNewline(reformatted)
	} else {
		Dependency.GetUi().DisplayTextNoNewline(reformatted)
	}
}

func newReader(rd io.Reader) IoReader {
	return bufio.NewReader(rd)
}
