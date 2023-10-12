package gpvui_test

import (
	"errors"
	"io"
	"regexp"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"

	"gitlab.eng.vmware.com/tanzu-data-services/greenplum/gp-virtual/gp-virtual-operator/pkg/constants"
	"gitlab.eng.vmware.com/tanzu-data-services/greenplum/gp-virtual/gp-virtual-operator/pkg/gpverror"
	"gitlab.eng.vmware.com/tanzu-data-services/greenplum/gp-virtual/gp-virtual-operator/pkg/gpvlog"
	"gitlab.eng.vmware.com/tanzu-data-services/greenplum/gp-virtual/gp-virtual-operator/pkg/gpvlog/gpvlogfakes"
	"gitlab.eng.vmware.com/tanzu-data-services/greenplum/gp-virtual/gp-virtual-operator/pkg/gpvui"
	"gitlab.eng.vmware.com/tanzu-data-services/greenplum/gp-virtual/gp-virtual-operator/pkg/gpvui/gpvuifakes"
)

var _ = Describe("gpvui package-level functions", func() {
	Context("DisplayTextToStream", func() {
		var testUi *gpvuifakes.FakeGpvUi

		BeforeEach(func() {
			gpvui.Default()
			testUi = &gpvuifakes.FakeGpvUi{}
			gpvui.Dependency.GetGpvUi = func() gpvui.GpvUi {
				return testUi
			}
		})

		When("the text is destined for the error stream", func() {
			It("prints text only to the error stream, and not to standard output", func() {
				gpvui.DisplayTextToStream(true, "test-error-text 25% done")

				Expect(testUi.DisplayErrorNoNewlineCallCount()).To(Equal(1))
				Expect(testUi.DisplayErrorNoNewlineArgsForCall(0)).To(Equal("test-error-text 25%% done"))
				Expect(testUi.DisplayTextNoNewlineCallCount()).To(Equal(0))
			})
		})

		When("the text is destined for the standard output stream", func() {
			It("prints text only to standard output, and not to the error stream", func() {
				gpvui.DisplayTextToStream(false, "test-text 25% done")

				Expect(testUi.DisplayTextNoNewlineCallCount()).To(Equal(1))
				Expect(testUi.DisplayTextNoNewlineArgsForCall(0)).To(Equal("test-text 25%% done"))
				Expect(testUi.DisplayErrorNoNewlineCallCount()).To(Equal(0))
			})
		})
	})
})

var _ = Describe("gpvui structs", func() {
	Context("GpvUiImpl", func() {
		var (
			testLogger *gpvlogfakes.FakeGpvLogger
			testGpvUi  *gpvui.GpvUiImpl
		)

		BeforeEach(func() {
			gpvui.Default()
			testLogger = &gpvlogfakes.FakeGpvLogger{}
			gpvui.Dependency.GetGpvLogger = func() gpvlog.GpvLogger {
				return testLogger
			}

			testGpvUi = &gpvui.GpvUiImpl{}
		})

		Context("DisplayError", func() {
			When("passed an error", func() {
				It("displays the error text to GpvUiImpl.Err, with a newline appended", func() {
					testGpvUi.SetErr(NewBuffer())

					testGpvUi.DisplayError("template with %s and\nthe integer %d", "an error", 45)

					Expect(testGpvUi.GetErr()).To(Say(regexp.QuoteMeta("template with an error and\nthe integer 45\n")))
					Expect(testLogger.ErrorCallCount()).To(Equal(2))
					Expect(testLogger.ErrorArgsForCall(0)).To(Equal([]any{"template with an error and"}))
					Expect(testLogger.ErrorArgsForCall(1)).To(Equal([]any{"the integer 45"}))
				})
			})
		})

		Context("DisplayErrorNoNewline", func() {
			When("passed an error", func() {
				It("displays the error text to GpvUiImpl.Err, with no newline appended", func() {
					testGpvUi.SetErr(NewBuffer())

					testGpvUi.DisplayErrorNoNewline("template with %s and\nthe integer %d", "an error", 45)

					Expect(testGpvUi.GetErr()).To(Say(regexp.QuoteMeta("template with an error and\nthe integer 45")))
					Expect(testLogger.ErrorCallCount()).To(Equal(2))
					Expect(testLogger.ErrorArgsForCall(0)).To(Equal([]any{"template with an error and"}))
					Expect(testLogger.ErrorArgsForCall(1)).To(Equal([]any{"the integer 45"}))
				})
			})
		})

		Context("DisplayPrompt", func() {
			When("presented with a string template and values to be substituted into the template", func() {
				It("displays the template with the values substituted within to GpvUiImpl.Out", func() {
					testGpvUi.SetOut(NewBuffer())

					testGpvUi.DisplayPrompt("template with %s and the integer %d:", "an error", 45)

					Expect(testGpvUi.GetOut()).To(Say("template with an error and the integer 45: "))
					Expect(testGpvUi.GetOut()).ToNot(Say("\n"))
					Expect(testLogger.InfoCallCount()).To(Equal(1))
					Expect(testLogger.InfoArgsForCall(0)).To(Equal([]any{"template with an error and the integer 45:"}))
				})
			})
		})

		Context("DisplayText", func() {
			When("presented with a string template and values to be substituted into the template", func() {
				It("displays the template with the values substituted within to GpvUiImpl.Out, with a newline appended", func() {
					testGpvUi.SetOut(NewBuffer())

					testGpvUi.DisplayText("template with %s and the integer %d", "an\nelf", 45)

					Expect(testGpvUi.GetOut()).To(Say("template with an\nelf and the integer 45\n"))
					Expect(testLogger.InfoCallCount()).To(Equal(2))
					Expect(testLogger.InfoArgsForCall(0)).To(Equal([]any{"template with an"}))
					Expect(testLogger.InfoArgsForCall(1)).To(Equal([]any{"elf and the integer 45"}))
				})
			})
		})

		Context("DisplayTextNoNewline", func() {
			When("presented with a string template and values to be substituted into the template", func() {
				It("displays the template with the values substituted within to GpvUiImpl.Out, with no newline appended", func() {
					testGpvUi.SetOut(NewBuffer())

					testGpvUi.DisplayTextNoNewline("template with %s and the integer %d", "an\nelf", 45)

					Expect(testGpvUi.GetOut()).To(Say("template with an\nelf and the integer 45"))
					Expect(testLogger.InfoCallCount()).To(Equal(2))
					Expect(testLogger.InfoArgsForCall(0)).To(Equal([]any{"template with an"}))
					Expect(testLogger.InfoArgsForCall(1)).To(Equal([]any{"elf and the integer 45"}))
				})
			})
		})

		Context("DisplayWarning", func() {
			When("passed a warning", func() {
				It("displays the warning text to GpvUiImpl.Out", func() {
					gpvui.Dependency.GetWarningFormat = func(warningCode constants.WarningCode) string {
						Expect(warningCode).To(Equal(constants.WarningCode(545)))
						return "test-warning-format %s %d"
					}
					testGpvUi.SetOut(NewBuffer())

					testGpvUi.DisplayWarning(545, "a", 8)

					Expect(testGpvUi.GetOut()).To(Say(regexp.QuoteMeta("WARN [0545] test-warning-format a 8\n")))
					Expect(testLogger.WarnCallCount()).To(Equal(1))
					Expect(testLogger.WarnArgsForCall(0)).To(Equal([]any{"WARN [0545] test-warning-format a 8"}))
				})
			})
		})

		Context("GetErr", func() {
			When("an error output writer exists in the struct", func() {
				It("matches the result of the function call", func() {
					testGpvUi.Err = NewBuffer()

					Expect(testGpvUi.GetErr()).To(Equal(testGpvUi.Err))
				})
			})
		})

		Context("GetErrLogWriter", func() {
			When("invoked", func() {
				It("returns a log writer for the error stream and log level 'error'", func() {
					expectedLogWriter := &gpvui.GpvStreamLogWriterImpl{IsErrStream: true}

					Expect(testGpvUi.GetErrLogWriter()).To(Equal(expectedLogWriter))
				})
			})
		})

		Context("GetIn", func() {
			When("an input io.Reader exists in the struct", func() {
				It("matches the result of the function call", func() {
					testGpvUi.In = NewBuffer()

					Expect(testGpvUi.GetIn()).To(Equal(testGpvUi.In))
				})
			})
		})

		Context("GetOut", func() {
			When("an output writer exists in the struct", func() {
				It("matches the result of the function call", func() {
					testGpvUi.Out = NewBuffer()

					Expect(testGpvUi.GetOut()).To(Equal(testGpvUi.Out))
				})
			})
		})

		Context("GetOutLogWriter", func() {
			When("invoked", func() {
				It("returns a log writer for the standard output stream and logg level 'info'", func() {
					expectedLogWriter := &gpvui.GpvStreamLogWriterImpl{IsErrStream: false}

					Expect(testGpvUi.GetOutLogWriter()).To(Equal(expectedLogWriter))
				})
			})
		})

		Context("ReadInput", func() {
			var (
				inBuff  *Buffer
				outBuff *Buffer
				reader  *gpvuifakes.FakeIoReader
			)

			BeforeEach(func() {
				// NOTE: The following is fragile
				inBuff = NewBuffer()
				reader = &gpvuifakes.FakeIoReader{}
				gpvui.Dependency.NewReader = func(rd io.Reader) gpvui.IoReader {
					Expect(rd).To(Equal(inBuff))
					return reader
				}
				testGpvUi.SetIn(inBuff)
				outBuff = NewBuffer()
				testGpvUi.SetOut(outBuff)
			})

			When("reading a string from input fails", func() {
				It("returns an error", func() {
					testErr := errors.New("failed to read")
					newErrorCallCount := 0
					expectedError := gpverror.New(759840)
					gpvui.Dependency.NewGpvError = func(errorCode constants.ErrorCode, args ...any) gpverror.Error {
						newErrorCallCount++
						Expect(errorCode).To(Equal(gpverror.FailedToGetUserInput))
						Expect(args).To(Equal([]any{testErr}))
						return expectedError
					}
					reader.ReadStringReturns("my input is cu\n", testErr)

					input, err := testGpvUi.ReadInput("test prompt")

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

					input1, err1 := testGpvUi.ReadInput("test prompt 1\n")
					input2, err2 := testGpvUi.ReadInput("test prompt 2\n")

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
				testGpvUi.SetOut(outBuff)
				gpvui.Dependency.ReadPasswordFromTerminal = func(fd int) ([]byte, error) {
					return []byte("user_password"), nil
				}
			})

			When("reading the password fails", func() {
				It("returns an error", func() {
					testErr := errors.New("read password failed")
					gpvui.Dependency.ReadPasswordFromTerminal = func(fd int) ([]byte, error) {
						return []byte{}, errors.New("read password failed")
					}
					newErrorCallCount := 0
					expectedError := gpverror.New(759840)
					gpvui.Dependency.NewGpvError = func(errorCode constants.ErrorCode, args ...any) gpverror.Error {
						newErrorCallCount++
						Expect(errorCode).To(Equal(gpverror.FailedToReadPassword))
						Expect(args).To(Equal([]any{testErr}))
						return expectedError
					}

					_, err := testGpvUi.ReadPassword("prompt for password:")

					Expect(err).To(MatchError(expectedError))
					Expect(newErrorCallCount).To(Equal(1))
				})
			})

			When("a prompt is passed as an argument", func() {
				It("displays the prompt and returns the input value", func() {
					input, err := testGpvUi.ReadPassword("prompt for password:")

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

					testGpvUi.SetErr(err)

					Expect(testGpvUi.Err).To(Equal(err))
				})
			})
		})

		Context("SetIn", func() {
			When("an input io.Reader is set", func() {
				It("matches the intended input io.Reader and creates an IoReader", func() {
					in := NewBuffer()
					reader := &gpvuifakes.FakeIoReader{}
					gpvui.Dependency.NewReader = func(rd io.Reader) gpvui.IoReader {
						Expect(rd).To(Equal(in))
						return reader
					}

					testGpvUi.SetIn(in)

					Expect(testGpvUi.In).To(Equal(in))
					Expect(testGpvUi.Reader).To(Equal(reader))
				})
			})
		})

		Context("SetOut", func() {
			When("an output writer is set", func() {
				It("matches the intended output writer", func() {
					out := NewBuffer()

					testGpvUi.SetOut(out)

					Expect(testGpvUi.Out).To(Equal(out))
				})
			})
		})
	})

	Context("GpvStreamLogWriterImpl", func() {
		Context("Write", func() {
			var (
				callCount        int
				testedText       string
				testStreamWriter *gpvui.GpvStreamLogWriterImpl
			)

			BeforeEach(func() {
				gpvui.Default()
				callCount = 0
				testedText = ""
				gpvui.Dependency.DisplayTextToStream = func(isErrStream bool, text string) {
					callCount++
					testedText = text
				}
				testStreamWriter = &gpvui.GpvStreamLogWriterImpl{}
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
