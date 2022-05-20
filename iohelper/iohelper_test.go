package iohelper_test

import (
	"errors"
	"io"
	"os"
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestIoHelper(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "iohelper tests")
}

var _ = BeforeSuite(func() {
	testhelper.SetupTestLogger()
})

var (
	errRead   = errors.New("Unable to open file for reading: Permission denied")
	errWrite  = errors.New("Unable to create or open file for writing: Permission denied")
	errAppend = errors.New("Unable to create or open file for appending: Permission denied")
)
var _ = Describe("operating/io tests", func() {
	AfterEach(func() {
		operating.InitializeSystemFunctions()
	})
	Describe("File reading and writing functions", func() {
		Describe("OpenFileForReading", func() {
			It("creates or opens the file for reading", func() {
				operating.System.OpenFileRead = func(name string, flag int, perm os.FileMode) (operating.ReadCloserAt, error) { return os.Stdin, nil }
				fileHandle, err := iohelper.OpenFileForReading("filename")
				Expect(err).ToNot(HaveOccurred())
				Expect(fileHandle).To(Equal(os.Stdin))
			})
			It("returns an error if one is generated", func() {
				operating.System.OpenFileRead = func(name string, flag int, perm os.FileMode) (operating.ReadCloserAt, error) {
					return nil, errors.New("Permission denied")
				}
				_, err := iohelper.OpenFileForReading("filename")
				Expect(err.Error()).To(Equal(errRead.Error()))
			})
		})
		Describe("MustOpenFileForReading", func() {
			It("creates or opens the file for reading", func() {
				operating.System.OpenFileRead = func(name string, flag int, perm os.FileMode) (operating.ReadCloserAt, error) { return os.Stdin, nil }
				fileHandle := iohelper.MustOpenFileForReading("filename")
				Expect(fileHandle).To(Equal(os.Stdin))
			})
			It("panics on error", func() {
				operating.System.OpenFileRead = func(name string, flag int, perm os.FileMode) (operating.ReadCloserAt, error) {
					return nil, errors.New("Permission denied")
				}
				defer testhelper.ShouldPanicWithMessage(errRead.Error())
				iohelper.MustOpenFileForReading("filename")
			})
		})
		Describe("OpenFileForWriting", func() {
			It("creates or opens the file for writing, and truncates any existing content", func() {
				var passedFlags int
				operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
					passedFlags = flag
					return os.Stdout, nil
				}
				fileHandle, err := iohelper.OpenFileForWriting("filename")
				Expect(err).ToNot(HaveOccurred())
				Expect(fileHandle).To(Equal(os.Stdout))
				Expect(passedFlags).To(Equal(os.O_CREATE | os.O_WRONLY | os.O_TRUNC))
			})
			It("returns an error if one is generated", func() {
				operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
					return nil, errors.New("Permission denied")
				}
				_, err := iohelper.OpenFileForWriting("filename")
				Expect(err.Error()).To(Equal(errWrite.Error()))
			})
		})
		Describe("MustOpenFileForWriting", func() {
			It("creates or opens the file for writing", func() {
				operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) { return os.Stdout, nil }
				fileHandle := iohelper.MustOpenFileForWriting("filename")
				Expect(fileHandle).To(Equal(os.Stdout))
			})
			It("panics on error", func() {
				operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
					return nil, errors.New("Permission denied")
				}
				defer testhelper.ShouldPanicWithMessage(errWrite.Error())
				iohelper.MustOpenFileForWriting("filename")
			})
		})
		Describe("OpenFileForAppending", func() {
			It("creates or opens the file for appending", func() {
				var passedFlags int
				operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
					passedFlags = flag
					return os.Stdout, nil
				}
				fileHandle, err := iohelper.OpenFileForAppending("filename")
				Expect(err).ToNot(HaveOccurred())
				Expect(fileHandle).To(Equal(os.Stdout))
				Expect(passedFlags).To(Equal(os.O_APPEND | os.O_CREATE | os.O_WRONLY))
			})
			It("returns an error if one is generated", func() {
				operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
					return nil, errors.New("Permission denied")
				}
				_, err := iohelper.OpenFileForAppending("filename")
				Expect(err.Error()).To(Equal(errAppend.Error()))
			})
		})
		Describe("MustOpenFileForAppending", func() {
			It("creates or opens the file for writing", func() {
				operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) { return os.Stdout, nil }
				fileHandle := iohelper.MustOpenFileForAppending("filename")
				Expect(fileHandle).To(Equal(os.Stdout))
			})
			It("panics on error", func() {
				operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
					return nil, errors.New("Permission denied")
				}
				defer testhelper.ShouldPanicWithMessage(errAppend.Error())
				iohelper.MustOpenFileForAppending("filename")
			})
		})
	})
	Describe("FileExistsAndIsReadable", func() {
		It("returns true if the file both exists and is readable", func() {
			operating.System.Stat = func(name string) (os.FileInfo, error) {
				return nil, nil
			}
			operating.System.OpenFileRead = func(name string, flag int, perm os.FileMode) (operating.ReadCloserAt, error) {
				return &os.File{}, nil
			}
			check := iohelper.FileExistsAndIsReadable("filename")
			Expect(check).To(BeTrue())
		})
		It("returns false if the file does not exist", func() {
			operating.System.Stat = func(name string) (os.FileInfo, error) {
				return nil, os.ErrNotExist
			}
			check := iohelper.FileExistsAndIsReadable("filename")
			Expect(check).To(BeFalse())
		})
		It("returns false if there is an error accessing the file", func() {
			operating.System.Stat = func(name string) (os.FileInfo, error) {
				return nil, os.ErrPermission
			}
			check := iohelper.FileExistsAndIsReadable("filename")
			Expect(check).To(BeFalse())
		})
		It("returns false if there is an error opening the file", func() {
			operating.System.Stat = func(name string) (os.FileInfo, error) {
				return nil, nil
			}
			operating.System.OpenFileRead = func(name string, flag int, perm os.FileMode) (operating.ReadCloserAt, error) {
				return &os.File{}, &os.PathError{}
			}
			check := iohelper.FileExistsAndIsReadable("filename")
			Expect(check).To(BeFalse())
		})
	})
	Describe("Reading file contents", func() {
		fileContents := `public.foo
public."bar%baz"`
		expectedContents := []string{`public.foo`, `public."bar%baz"`}

		Describe("ReadLinesFromFile", func() {
			It("reads a file containing multiple lines", func() {
				testhelper.MockFileContents(fileContents)
				contents, err := iohelper.ReadLinesFromFile("/tmp/table_file")
				Expect(err).ToNot(HaveOccurred())
				Expect(contents).To(Equal(expectedContents))
			})
			It("returns an empty array if the file is empty", func() {
				testhelper.MockFileContents("")
				contents, err := iohelper.ReadLinesFromFile("/tmp/table_file")
				Expect(err).ToNot(HaveOccurred())
				Expect(contents).To(Equal([]string{}))
			})
			It("returns an error if there is an error reading the file", func() {
				operating.System.OpenFileRead = func(name string, flag int, perm os.FileMode) (operating.ReadCloserAt, error) {
					return nil, errors.New("Permission denied")
				}
				contents, err := iohelper.ReadLinesFromFile("/tmp/table_file")
				Expect(err.Error()).To(Equal(errRead.Error()))
				Expect(contents).To(BeNil())
			})
			It("returns an error if the file cannot be closed", func() {
				operating.System.OpenFileRead = func(name string, flag int, perm os.FileMode) (operating.ReadCloserAt, error) {
					myCloser := FailsClosing{}
					return myCloser, nil
				}
				contents, err := iohelper.ReadLinesFromFile("/tmp/table_file")
				Expect(err).To(HaveOccurred())

				Expect(err.Error()).To(Equal("intentional closing failure"))
				Expect(contents).To(BeNil())
			})
		})
		Describe("MustReadLinesFromFile", func() {
			It("reads a file containing multiple lines", func() {
				testhelper.MockFileContents(fileContents)
				contents := iohelper.MustReadLinesFromFile("/tmp/table_file")
				Expect(contents).To(Equal(expectedContents))
			})
			It("returns an empty array if the file is empty", func() {
				testhelper.MockFileContents("")
				contents := iohelper.MustReadLinesFromFile("/tmp/table_file")
				Expect(contents).To(Equal([]string{}))
			})
			It("panics if there is an error reading the file", func() {
				operating.System.OpenFileRead = func(name string, flag int, perm os.FileMode) (operating.ReadCloserAt, error) {
					return nil, errors.New("Permission denied")
				}
				defer testhelper.ShouldPanicWithMessage(errRead.Error())
				iohelper.MustReadLinesFromFile("/tmp/table_file")
			})
		})
	})
})

type FailsClosing struct{}

func (fc FailsClosing) Read(p []byte) (n int, err error) {
	return 1, nil
}
func (fc FailsClosing) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, nil
}
func (fc FailsClosing) Close() error {
	return errors.New("intentional closing failure")
}
