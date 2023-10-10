package ui_test

import (
	"errors"
	"io"
	"regexp"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"

	"github.com/greenplum-db/gp-common-go-libs/constants"
	"github.com/greenplum-db/gp-common-go-libs/error"
	"github.com/greenplum-db/gp-common-go-libs/log/logfakes"
	"github.com/greenplum-db/gp-common-go-libs/ui"
	"github.com/greenplum-db/gp-common-go-libs/ui/uifakes"
)

var _ = Describe("ui package-level functions", func() {
	Context("DisplayTextToStream", func() {
		var testUi *uifakes.Fakeui

		BeforeEach(func() {
			ui.Default()
			testUi = &uifakes.Fakeui{}
			ui.Dependency.GetUi = func() ui.Ui {
				return testUi
			}
		})

		When("the text is destined for the error stream", func() {
			It("prints text only to the error stream, and not to standard output", func() {
				ui.DisplayTextToStream(true, "test-error-text 25% done")

				Expect(testUi.DisplayErrorNoNewlineCallCount()).To(Equal(1))
				Expect(testUi.DisplayErrorNoNewlineArgsForCall(0)).To(Equal("test-error-text 25%% done"))
				Expect(testUi.DisplayTextNoNewlineCallCount()).To(Equal(0))
			})
		})

		When("the text is destined for the standard output stream", func() {
			It("prints text only to standard output, and not to the error stream", func() {
				ui.DisplayTextToStream(false, "test-text 25% done")

				Expect(testUi.DisplayTextNoNewlineCallCount()).To(Equal(1))
				Expect(testUi.DisplayTextNoNewlineArgsForCall(0)).To(Equal("test-text 25%% done"))
				Expect(testUi.DisplayErrorNoNewlineCallCount()).To(Equal(0))
			})
		})
	})
})

var _ = Describe("ui structs", func() {
	Context("UiImpl", func() {
		var (
			testLogger *logfakes.Fakelogger
			testui     *ui.UiImpl
		)

		BeforeEach(func() {
			ui.Default()
			testLogger = &logfakes.Fakelogger{}
			ui.Dependency.Getlogger = func() log.logger {
				return testLogger
			}

			testui = &ui.UiImpl{}
		})

		Context("DisplayError", func() {
			When("passed an error", func() {
				It("displays the error text to UiImpl.Err, with a newline appended", func() {
					testui.SetErr(NewBuffer())

					testui.DisplayError("template with %s and\nthe integer %d", "an error", 45)

					Expect(testui.GetErr()).To(Say(regexp.QuoteMeta("template with an error and\nthe integer 45\n")))
					Expect(testLogger.ErrorCallCount()).To(Equal(2))
					Expect(testLogger.ErrorArgsForCall(0)).To(Equal([]any{"template with an error and"}))
					Expect(testLogger.ErrorArgsForCall(1)).To(Equal([]any{"the integer 45"}))
				})
			})
		})

		Context("DisplayErrorNoNewline", func() {
			When("passed an error", func() {
				It("displays the error text to UiImpl.Err, with no newline appended", func() {
					testui.SetErr(NewBuffer())

					testui.DisplayErrorNoNewline("template with %s and\nthe integer %d", "an error", 45)

					Expect(testui.GetErr()).To(Say(regexp.QuoteMeta("template with an error and\nthe integer 45")))
					Expect(testLogger.ErrorCallCount()).To(Equal(2))
					Expect(testLogger.ErrorArgsForCall(0)).To(Equal([]any{"template with an error and"}))
					Expect(testLogger.ErrorArgsForCall(1)).To(Equal([]any{"the integer 45"}))
				})
			})
		})

		Context("DisplayPrompt", func() {
			When("presented with a string template and values to be substituted into the template", func() {
				It("displays the template with the values substituted within to UiImpl.Out", func() {
					testui.SetOut(NewBuffer())

					testui.DisplayPrompt("template with %s and the integer %d:", "an error", 45)

					Expect(testui.GetOut()).To(Say("template with an error and the integer 45: "))
					Expect(testui.GetOut()).ToNot(Say("\n"))
					Expect(testLogger.InfoCallCount()).To(Equal(1))
					Expect(testLogger.InfoArgsForCall(0)).To(Equal([]any{"template with an error and the integer 45:"}))
				})
			})
		})

		Context("DisplayText", func() {
			When("presented with a string template and values to be substituted into the template", func() {
				It("displays the template with the values substituted within to UiImpl.Out, with a newline appended", func() {
					testui.SetOut(NewBuffer())

					testui.DisplayText("template with %s and the integer %d", "an\nelf", 45)

					Expect(testui.GetOut()).To(Say("template with an\nelf and the integer 45\n"))
					Expect(testLogger.InfoCallCount()).To(Equal(2))
					Expect(testLogger.InfoArgsForCall(0)).To(Equal([]any{"template with an"}))
					Expect(testLogger.InfoArgsForCall(1)).To(Equal([]any{"elf and the integer 45"}))
				})
			})
		})

		Context("DisplayTextNoNewline", func() {
			When("presented with a string template and values to be substituted into the template", func() {
				It("displays the template with the values substituted within to UiImpl.Out, with no newline appended", func() {
					testui.SetOut(NewBuffer())

					testui.DisplayTextNoNewline("template with %s and the integer %d", "an\nelf", 45)

					Expect(testui.GetOut()).To(Say("template with an\nelf and the integer 45"))
					Expect(testLogger.InfoCallCount()).To(Equal(2))
					Expect(testLogger.InfoArgsForCall(0)).To(Equal([]any{"template with an"}))
					Expect(testLogger.InfoArgsForCall(1)).To(Equal([]any{"elf and the integer 45"}))
				})
			})
		})

		Context("DisplayWarning", func() {
			When("passed a warning", func() {
				It("displays the warning text to UiImpl.Out", func() {
					ui.Dependency.GetWarningFormat = func(warningCode constants.WarningCode) string {
						Expect(warningCode).To(Equal(constants.WarningCode(545)))
						return "test-warning-format %s %d"
					}
					testui.SetOut(NewBuffer())

					testui.DisplayWarning(545, "a", 8)

					Expect(testui.GetOut()).To(Say(regexp.QuoteMeta("WARN [0545] test-warning-format a 8\n")))
					Expect(testLogger.WarnCallCount()).To(Equal(1))
					Expect(testLogger.WarnArgsForCall(0)).To(Equal([]any{"WARN [0545] test-warning-format a 8"}))
				})
			})
		})

		Context("GetErr", func() {
			When("an error output writer exists in the struct", func() {
				It("matches the result of the function call", func() {
					testui.Err = NewBuffer()

					Expect(testui.GetErr()).To(Equal(testui.Err))
				})
			})
		})

		Context("GetErrLogWriter", func() {
			When("invoked", func() {
				It("returns a log writer for the error stream and log level 'error'", func() {
					expectedLogWriter := &ui.GpvStreamLogWriterImpl{IsErrStream: true}

					Expect(testui.GetErrLogWriter()).To(Equal(expectedLogWriter))
				})
			})
		})

		Context("GetIn", func() {
			When("an input io.Reader exists in the struct", func() {
				It("matches the result of the function call", func() {
					testui.In = NewBuffer()

					Expect(testui.GetIn()).To(Equal(testui.In))
				})
			})
		})

		Context("GetOut", func() {
			When("an output writer exists in the struct", func() {
				It("matches the result of the function call", func() {
					testui.Out = NewBuffer()

					Expect(testui.GetOut()).To(Equal(testui.Out))
				})
			})
		})

		Context("GetOutLogWriter", func() {
			When("invoked", func() {
				It("returns a log writer for the standard output stream and logg level 'info'", func() {
					expectedLogWriter := &ui.GpvStreamLogWriterImpl{IsErrStream: false}

					Expect(testui.GetOutLogWriter()).To(Equal(expectedLogWriter))
				})
			})
		})

		Context("ReadInput", func() {
			var (
				inBuff  *Buffer
				outBuff *Buffer
				reader  *uifakes.FakeIoReader
			)

			BeforeEach(func() {
				// NOTE: The following is fragile
				inBuff = NewBuffer()
				reader = &uifakes.FakeIoReader{}
				ui.Dependency.NewReader = func(rd io.Reader) ui.IoReader {
					Expect(rd).To(Equal(inBuff))
					return reader
				}
				testui.SetIn(inBuff)
				outBuff = NewBuffer()
				testui.SetOut(outBuff)
			})

			When("reading a string from input fails", func() {
				It("returns an error", func() {
					testErr := errors.New("failed to read")
					newErrorCallCount := 0
					expectedError := error.New(759840)
					ui.Dependency.NewError = func(errorCode constants.ErrorCode, args ...any) error.Error {
						newErrorCallCount++
						Expect(errorCode).To(Equal(error.FailedToGetUserInput))
						Expect(args).To(Equal([]any{testErr}))
						return expectedError
					}
					reader.ReadStringReturns("my input is cu\n", testErr)

					input, err := testui.ReadInput("test prompt")

					Expect(err).To(MatchError(expectedError))
					Expect(newErrorCallCount).To(Equal(1))
					Expect(input).To(Equal(""))
					Expect(outBuff).To(Say("test prompt"))
				})
			})

			When("reading multiple strings from input succeeds", func() {
				It("returns the strings without errors", func() {
					reader.ReadStringReturnsOnCall(0, "input 1\n", nil)
					reader.ReadStringReturnsOnCall(1, "input 5\n", nil)

					input1, err1 := testui.ReadInput("test prompt 1\n")
					input2, err2 := testui.ReadInput("test prompt 2\n")

					Expect(err1).ToNot(HaveOccurred())
					Expect(err2).ToNot(HaveOccurred())
					Expect(input1).To(Equal("input 1"))
					Expect(input2).To(Equal("input 5"))
					Expect(outBuff).To(Say("test prompt 1\n"))
					Expect(outBuff).To(Say("test prompt 2\n"))
				})
			})
		})

		Context("ReadPassword", func() {
			var (
				outBuff *Buffer
			)

			BeforeEach(func() {
				outBuff = NewBuffer()
				testui.SetOut(outBuff)
				ui.Dependency.ReadPasswordFromTerminal = func(fd int) ([]byte, error) {
					return []byte("user_password"), nil
				}
			})

			When("reading the password fails", func() {
				It("returns an error", func() {
					testErr := errors.New("read password failed")
					ui.Dependency.ReadPasswordFromTerminal = func(fd int) ([]byte, error) {
						return []byte{}, errors.New("read password failed")
					}
					newErrorCallCount := 0
					expectedError := error.New(759840)
					ui.Dependency.NewError = func(errorCode constants.ErrorCode, args ...any) error.Error {
						newErrorCallCount++
						Expect(errorCode).To(Equal(error.FailedToReadPassword))
						Expect(args).To(Equal([]any{testErr}))
						return expectedError
					}

					_, err := testui.ReadPassword("prompt for password:")

					Expect(err).To(MatchError(expectedError))
					Expect(newErrorCallCount).To(Equal(1))
				})
			})

			When("a prompt is passed as an argument", func() {
				It("displays the prompt and returns the input value", func() {
					input, err := testui.ReadPassword("prompt for password:")

					Expect(err).ToNot(HaveOccurred())
					Expect(outBuff).To(Say("prompt for password: \n"))
					Expect(input).To(Equal("user_password"))
				})
			})
		})

		Context("SetErr", func() {
			When("an error output writer is set", func() {
				It("matches the intended error output writer", func() {
					err := NewBuffer()

					testui.SetErr(err)

					Expect(testui.Err).To(Equal(err))
				})
			})
		})

		Context("SetIn", func() {
			When("an input io.Reader is set", func() {
				It("matches the intended input io.Reader and creates an IoReader", func() {
					in := NewBuffer()
					reader := &uifakes.FakeIoReader{}
					ui.Dependency.NewReader = func(rd io.Reader) ui.IoReader {
						Expect(rd).To(Equal(in))
						return reader
					}

					testui.SetIn(in)

					Expect(testui.In).To(Equal(in))
					Expect(testui.Reader).To(Equal(reader))
				})
			})
		})

		Context("SetOut", func() {
			When("an output writer is set", func() {
				It("matches the intended output writer", func() {
					out := NewBuffer()

					testui.SetOut(out)

					Expect(testui.Out).To(Equal(out))
				})
			})
		})
	})

	Context("GpvStreamLogWriterImpl", func() {
		Context("Write", func() {
			var (
				callCount        int
				testedText       string
				testStreamWriter *ui.GpvStreamLogWriterImpl
			)

			BeforeEach(func() {
				ui.Default()
				callCount = 0
				testedText = ""
				ui.Dependency.DisplayTextToStream = func(isErrStream bool, text string) {
					callCount++
					testedText = text
				}
				testStreamWriter = &ui.GpvStreamLogWriterImpl{}
			})

			When("it is called with an empty byte array", func() {
				It("displays nothing", func() {
					p, err := testStreamWriter.Write([]byte{})

					Expect(err).ToNot(HaveOccurred())
					Expect(p).To(Equal(0))
					Expect(callCount).To(Equal(0))
				})
			})

			When("it is called with a multi-line []byte containing percent signs and no trailing newline", func() {
				It("displays the lines in order and doubles the percent signs to prevent unwanted attempts at formatting", func() {
					p, err := testStreamWriter.Write([]byte("\nfoo 25%\nbar"))

					Expect(err).ToNot(HaveOccurred())
					Expect(p).To(Equal(12))
					Expect(callCount).To(Equal(1))
					Expect(testedText).To(Equal("\nfoo 25%\nbar"))
				})
			})
		})
	})
})
