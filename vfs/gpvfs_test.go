package vfs_test

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"

	"github.com/blang/vfs"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/greenplum-db/gp-common-go-libs/constants"
	"github.com/greenplum-db/gp-common-go-libs/error"
	"github.com/greenplum-db/gp-common-go-libs/vfs"
	"github.com/greenplum-db/gp-common-go-libs/vfs/vfsfakes"
)

var _ = Describe("vfs", func() {
	var (
		expectedError     error.Error
		expectedErrorArgs []any
		expectedErrorCode constants.ErrorCode
		newErrorCallCount int
	)

	BeforeEach(func() {
		vfs.Default()
		vfs.SetTestFs()

		newErrorCallCount = 0
		expectedError = error.New(7685940)
		vfs.Dependency.NewError = func(errorCode constants.ErrorCode, args ...any) error.Error {
			newErrorCallCount++
			Expect(errorCode).To(Equal(expectedErrorCode))
			Expect(args).To(Equal(expectedErrorArgs))
			return expectedError
		}
	})

	Context("AppendFile", func() {
		var (
			testFile *vfsfakes.FakeGpvFile
		)

		BeforeEach(func() {
			testFile = &vfsfakes.FakeGpvFile{}

			vfs.Dependency.EnsureParentDirectoryExists = func(fullPath string) error {
				Expect(fullPath).To(Equal("test-path/test-filename"))
				return nil
			}
			vfs.Dependency.OpenFile = func(fullPath string, flag int, perm os.FileMode) (vfs.GpvFile, error) {
				Expect(fullPath).To(Equal("test-path/test-filename"))
				Expect(flag).To(Equal(os.O_WRONLY | os.O_CREATE | os.O_APPEND))
				Expect(perm).To(Equal(os.FileMode(0600)))
				return testFile, nil
			}
		})

		When("existence of the parent directory cannot be ensured", func() {
			It("returns an error", func() {
				vfs.Dependency.EnsureParentDirectoryExists = func(fullPath string) error {
					return errors.New("unable to access directory")
				}

				byteCount, err := vfs.AppendFile("test-path/test-filename", []byte("test-data"))

				Expect(err).To(MatchError("unable to access directory"))
				Expect(byteCount).To(Equal(0))
			})
		})

		When("the file cannot be opened", func() {
			It("returns an error", func() {
				vfs.Dependency.OpenFile = func(fullPath string, flag int, perm os.FileMode) (vfs.GpvFile, error) {
					return nil, errors.New("unable to open file")
				}

				byteCount, err := vfs.AppendFile("test-path/test-filename", []byte("test-data"))

				Expect(err).To(MatchError("unable to open file"))
				Expect(byteCount).To(Equal(0))
			})
		})

		When("the data are written to the file but an error occurs", func() {
			It("returns number of bytes and a error.Error", func() {
				testErr := errors.New("write failed")
				testFile.WriteReturns(42, testErr)
				expectedErrorCode = error.FileSystemIssue
				expectedErrorArgs = []any{testErr}

				byteCount, err := vfs.AppendFile("test-path/test-filename", []byte("test-data"))

				Expect(err).To(MatchError(expectedError))
				Expect(newErrorCallCount).To(Equal(1))
				Expect(byteCount).To(Equal(42))
				Expect(testFile.CloseCallCount()).To(Equal(1))
				Expect(testFile.WriteCallCount()).To(Equal(1))
				Expect(string(testFile.WriteArgsForCall(0))).To(Equal("test-data"))
			})
		})

		When("the data are written to the file and no error occurs", func() {
			It("returns number of bytes", func() {
				testFile.WriteReturns(55, nil)

				byteCount, err := vfs.AppendFile("test-path/test-filename", []byte("test-data"))

				Expect(err).ToNot(HaveOccurred())
				Expect(byteCount).To(Equal(55))
				Expect(testFile.CloseCallCount()).To(Equal(1))
				Expect(testFile.WriteCallCount()).To(Equal(1))
				Expect(string(testFile.WriteArgsForCall(0))).To(Equal("test-data"))
			})
		})
	})

	Context("Chmod", func() {
		When("the filesystem has been substituted with a fake filesystem", func() {
			It("returns no error", func() {
				Expect(vfs.Chmod("test", 0124)).To(Succeed())
			})
		})

		When("the filesystem has not been substituted with a fake filesystem", func() {
			It("returns the results of the OS's Chmod", func() {
				vfs.SetFS()
				vfs.Dependency.OsChmod = func(filename string, mode os.FileMode) error {
					Expect(filename).To(Equal("test"))
					Expect(mode).To(Equal(os.FileMode(0124)))
					return errors.New("chmod failed")
				}

				Expect(vfs.Chmod("test", 0124)).To(MatchError("chmod failed"))
			})
		})
	})

	Context("EnsureParentDirectoryExists", func() {
		BeforeEach(func() {
			vfs.Dependency.MkdirAll = func(dirPath string, perm os.FileMode) error {
				Expect(dirPath).To(Equal("test-path"))
				Expect(perm).To(Equal(os.FileMode(0700)))
				return nil
			}
		})

		When("the dir path cannot be created", func() {
			It("returns an error", func() {
				testErr := errors.New("failed to make all dirs")
				vfs.Dependency.MkdirAll = func(dirPath string, perm os.FileMode) error {
					return testErr
				}
				expectedErrorCode = error.FileSystemIssue
				expectedErrorArgs = []any{testErr}

				err := vfs.EnsureParentDirectoryExists("test-path/test-filename")

				Expect(err).To(MatchError(expectedError))
				Expect(newErrorCallCount).To(Equal(1))
			})
		})

		When("the file already exists at the given path", func() {
			It("does not attempt to create the file", func() {
				Expect(vfs.EnsureParentDirectoryExists("test-path/test-filename")).ToNot(HaveOccurred())
			})
		})
	})

	Context("GetChecksum", func() {
		var (
			testFile    *vfsfakes.FakeGpvFile
			testShaHash *vfsfakes.FakeShaHash
		)

		BeforeEach(func() {
			testFile = &vfsfakes.FakeGpvFile{}
			testShaHash = &vfsfakes.FakeShaHash{}

			vfs.Dependency.OpenFile = func(fullPath string, flag int, _ os.FileMode) (vfs.GpvFile, error) {
				Expect(fullPath).To(Equal("test-full-path"))
				Expect(flag).To(Equal(os.O_RDONLY))
				return testFile, nil
			}
			vfs.Dependency.GetSha256Hash = func() vfs.ShaHash {
				return testShaHash
			}
			vfs.Dependency.StreamCopy = func(dst io.Writer, src io.Reader) (written int64, err error) {
				Expect(dst).To(Equal(testShaHash))
				Expect(src).To(Equal(testFile))
				return 0, nil
			}
			testShaHash.SumReturns([]uint8("test-bytes"))
			vfs.Dependency.HexEncodeToString = func(src []byte) string {
				Expect(src).To(Equal([]uint8("test-bytes")))
				return "test-checksum"
			}
		})

		When("the file cannot be opened", func() {
			It("returns an error", func() {
				vfs.Dependency.OpenFile = func(fullPath string, flag int, _ os.FileMode) (vfs.GpvFile, error) {
					return nil, errors.New("unable to open file")
				}

				_, err := vfs.GetChecksum("test-full-path")

				Expect(err).To(MatchError("unable to open file"))
			})
		})

		When("the file cannot be hashed", func() {
			It("returns an error", func() {
				testErr := errors.New("failed to stream hash")
				vfs.Dependency.StreamCopy = func(dst io.Writer, src io.Reader) (written int64, err error) {
					return 0, testErr
				}
				expectedErrorCode = error.FailedToComputeChecksumForFile
				expectedErrorArgs = []any{"test-full-path", testErr}

				_, err := vfs.GetChecksum("test-full-path")

				Expect(err).To(MatchError(expectedError))
				Expect(newErrorCallCount).To(Equal(1))
				Expect(testFile.CloseCallCount()).To(Equal(1))
			})
		})

		When("the checksum is obtained", func() {
			It("returns the checksum", func() {
				checksum, err := vfs.GetChecksum("test-full-path")

				Expect(err).ToNot(HaveOccurred())
				Expect(checksum).To(Equal("test-checksum"))
				Expect(testFile.CloseCallCount()).To(Equal(1))
			})
		})
	})

	Context("OpenFile", func() {
		var (
			testGpvFilesystem *vfsfakes.FakeGpvFilesystem
		)

		BeforeEach(func() {
			testGpvFilesystem = &vfsfakes.FakeGpvFilesystem{}
			vfs.Dependency.GetFs = func() vfs.GpvFilesystem {
				return testGpvFilesystem
			}
		})

		When("opening a file fails", func() {
			It("returns an error", func() {
				testErr := errors.New("failed to open")
				testGpvFilesystem.OpenFileReturns(nil, testErr)
				expectedErrorCode = error.FileSystemIssue
				expectedErrorArgs = []any{testErr}
				vfs.Dependency.GetFs = func() vfs.GpvFilesystem {
					return testGpvFilesystem
				}

				file, err := vfs.OpenFile("test-path", os.O_SYNC|os.O_CREATE|os.O_RDONLY, 0123)

				Expect(err).To(MatchError(expectedError))
				Expect(newErrorCallCount).To(Equal(1))
				Expect(file).To(BeNil())
				Expect(testGpvFilesystem.OpenFileCallCount()).To(Equal(1))
				path, flags, perms := testGpvFilesystem.OpenFileArgsForCall(0)
				Expect(path).To(Equal("test-path"))
				Expect(flags).To(Equal(os.O_SYNC | os.O_CREATE | os.O_RDONLY))
				Expect(perms).To(Equal(fs.FileMode(0123)))
			})
		})

		When("opening a file succeeds", func() {
			It("returns no error", func() {
				testGpvFile := &vfsfakes.FakeGpvFile{}
				testGpvFilesystem.OpenFileReturns(testGpvFile, nil)

				file, err := vfs.OpenFile("test-path", os.O_SYNC|os.O_CREATE|os.O_RDONLY, 0123)

				Expect(err).ToNot(HaveOccurred())
				Expect(file).To(Equal(testGpvFile))
				Expect(testGpvFilesystem.OpenFileCallCount()).To(Equal(1))
				path, flags, perms := testGpvFilesystem.OpenFileArgsForCall(0)
				Expect(path).To(Equal("test-path"))
				Expect(flags).To(Equal(os.O_SYNC | os.O_CREATE | os.O_RDONLY))
				Expect(perms).To(Equal(fs.FileMode(0123)))
			})
		})
	})

	Context("ReadFile", func() {
		BeforeEach(func() {
			vfs.Dependency.ReadFilePrivate = func(fs vfs.Filesystem, filename string) ([]byte, error) {
				Expect(filename).To(Equal("/foo/bar"))
				return []byte("test-data"), nil
			}
		})

		When("reading the file fails", func() {
			It("returns an error", func() {
				testErr := errors.New("read file failed")
				vfs.Dependency.ReadFilePrivate = func(fs vfs.Filesystem, filename string) ([]byte, error) {
					Expect(filename).To(Equal("/foo/bar"))
					return nil, testErr
				}
				expectedErrorCode = error.FailedToReadFile
				expectedErrorArgs = []any{"/foo/bar", testErr}

				_, err := vfs.ReadFile("/foo/bar")

				Expect(err).To(MatchError(expectedError))
				Expect(newErrorCallCount).To(Equal(1))
			})
		})

		When("reading the file succeeds", func() {
			It("returns the content of the file", func() {
				data, err := vfs.ReadFile("/foo/bar")

				Expect(err).ToNot(HaveOccurred())
				Expect(string(data)).To(Equal("test-data"))
			})
		})
	})

	Context("WriteFile", func() {
		When("writing the file fails", func() {
			It("returns an error", func() {
				vfs.Dependency.WriteFileWithPermissions = func(fullPath string, data []byte, mode os.FileMode) error {
					Expect(fullPath).To(Equal("foo/bar/test-file"))
					Expect(data).To(Equal([]byte("data")))
					Expect(mode).To(Equal(os.FileMode(0600)))

					return errors.New("writing to file failed")
				}

				Expect(vfs.WriteFile("foo/bar/test-file", []byte("data"))).To(MatchError("writing to file failed"))
			})
		})
	})

	Context("WriteFileWithPermissions", func() {
		var removeCallCount int

		BeforeEach(func() {
			removeCallCount = 0

			vfs.Dependency.EnsureParentDirectoryExists = func(fullPath string) error {
				Expect(fullPath).To(Equal("test-path/test-filename"))
				return nil
			}
			vfs.Dependency.WriteVfsFile = func(filename string, data []byte, perm os.FileMode) error {
				Expect(filename).To(Equal("test-path/test-filename.tmp"))
				Expect(string(data)).To(Equal("test-data"))
				Expect(perm).To(Equal(os.FileMode(0004)))
				return nil
			}
			vfs.Dependency.Chmod = func(filename string, mode os.FileMode) error {
				Expect(filename).To(Equal("test-path/test-filename.tmp"))
				Expect(mode).To(Equal(os.FileMode(0004)))
				return nil
			}
			vfs.Dependency.Stat = func(fullPath string) (os.FileInfo, error) {
				Expect(fullPath).To(Equal("test-path/test-filename"))
				return nil, nil
			}
			vfs.Dependency.RenameToOld = func(currentPath string, currentPathDotOld string) error {
				Expect(currentPath).To(Equal("test-path/test-filename"))
				Expect(currentPathDotOld).To(Equal("test-path/test-filename.old"))
				return nil
			}
			vfs.Dependency.RenameFromTmp = func(currentPathDotTmp string, currentPath string) error {
				Expect(currentPathDotTmp).To(Equal("test-path/test-filename.tmp"))
				Expect(currentPath).To(Equal("test-path/test-filename"))
				return nil
			}
			vfs.Dependency.Remove = func(filename string) {
				removeCallCount++
				Expect(filename).To(Equal("test-path/test-filename.old"))
			}
		})

		When("ensuring the parent directory's existence fails", func() {
			It("returns an error", func() {
				vfs.Dependency.EnsureParentDirectoryExists = func(fullPath string) error {
					return errors.New("could not ensure existence")
				}

				err := vfs.WriteFileWithPermissions("test-path/test-filename", []byte("test-data"), 0004)

				Expect(err).To(MatchError("could not ensure existence"))
			})
		})

		When("the content of the file cannot be witten to a temporary location", func() {
			It("returns an error", func() {
				testErr := errors.New("failed to write")
				vfs.Dependency.WriteVfsFile = func(filename string, data []byte, perm os.FileMode) error {
					return testErr
				}
				expectedErrorCode = error.FileSystemIssue
				expectedErrorArgs = []any{testErr}

				err := vfs.WriteFileWithPermissions("test-path/test-filename", []byte("test-data"), 0004)

				Expect(err).To(MatchError(expectedError))
				Expect(newErrorCallCount).To(Equal(1))
			})
		})

		When("the new file's permissions cannot be set by chmod", func() {
			It("returns an error", func() {
				testErr := errors.New("failed to change perms")
				vfs.Dependency.Chmod = func(filename string, mode os.FileMode) error {
					return testErr
				}
				expectedErrorCode = error.FileSystemIssue
				expectedErrorArgs = []any{testErr}

				err := vfs.WriteFileWithPermissions("test-path/test-filename", []byte("test-data"), 0004)

				Expect(err).To(MatchError(expectedError))
				Expect(newErrorCallCount).To(Equal(1))
			})
		})

		When("the file previously did not exist", func() {
			It("does not attempt to rename it as (x->x.old)", func() {
				vfs.Dependency.Stat = func(fullPath string) (os.FileInfo, error) {
					return nil, errors.New("file does not exist")
				}
				vfs.Dependency.RenameToOld = func(currentPath string, currentPathDotOld string) error {
					return errors.New("should not rename")
				}

				err := vfs.WriteFileWithPermissions("test-path/test-filename", []byte("test-data"), 0004)

				Expect(err).ToNot(HaveOccurred())
			})
		})

		When("the file cannot be moved to another location (x->x.old)", func() {
			It("returns an error", func() {
				testErr := errors.New("failed to rename old")
				vfs.Dependency.RenameToOld = func(currentPath string, currentPathDotOld string) error {
					return testErr
				}
				expectedErrorCode = error.FileSystemIssue
				expectedErrorArgs = []any{testErr}

				err := vfs.WriteFileWithPermissions("test-path/test-filename", []byte("test-data"), 0004)

				Expect(err).To(MatchError(expectedError))
				Expect(newErrorCallCount).To(Equal(1))
			})
		})

		When("the file cannot be moved to another location (x.tmp->x)", func() {
			It("returns an error", func() {
				testErr := errors.New("failed to rename tmp")
				vfs.Dependency.RenameFromTmp = func(currentPathDotTmp string, currentPath string) error {
					return testErr
				}
				expectedErrorCode = error.FileSystemIssue
				expectedErrorArgs = []any{testErr}

				err := vfs.WriteFileWithPermissions("test-path/test-filename", []byte("test-data"), 0004)

				Expect(err).To(MatchError(expectedError))
				Expect(newErrorCallCount).To(Equal(1))
				Expect(removeCallCount).To(Equal(0))
			})
		})

		When("writing the file succeeds", func() {
			It("removes the old file and returns no error", func() {
				Expect(vfs.WriteFileWithPermissions("test-path/test-filename", []byte("test-data"), 0004)).To(Succeed())
				Expect(removeCallCount).To(Equal(1))
			})
		})
	})

	Context("WriteFileWithPermissions integrative tests", func() {
		var (
			testFs  vfs.Filesystem
			dirPath string
		)

		BeforeEach(func() {
			testFs = vfs.SetTestFs()
			vfs.Dependency.Stat = testFs.Stat

			vfs.Dependency.Chmod = func(name string, mode os.FileMode) error {
				return nil
			}
			dirPath = "/tmp"
		})

		AfterEach(func() {
			vfs.SetFS()
		})

		When("no pre-existing config.yml is present", func() {
			It("writes the file in the expected path", func() {
				fullPath := path.Join(dirPath, "config.yml")
				err := vfs.WriteFileWithPermissions(fullPath, []byte("something"), 0600)
				Expect(err).ToNot(HaveOccurred())

				fileInfo, err := testFs.Stat(fullPath)
				Expect(fileInfo).ToNot(BeNil())
				Expect(err).ToNot(HaveOccurred())

				tmpPath := fmt.Sprintf("%s.tmp", fullPath)
				fileInfo, err = testFs.Stat(tmpPath)
				Expect(fileInfo).To(BeNil())
				Expect(err).To(HaveOccurred())

				oldPath := fmt.Sprintf("%s.old", fullPath)
				fileInfo, err = testFs.Stat(oldPath)
				Expect(fileInfo).To(BeNil())
				Expect(err).To(HaveOccurred())
			})
		})

		When("a pre-existing config.yml is present", func() {
			It("writes the file in the expected path", func() {
				fullPath := path.Join(dirPath, "config.yml")
				err := vfs.MkdirAll(testFs, dirPath, 0700)
				Expect(err).ToNot(HaveOccurred())
				err = vfs.WriteFile(testFs, fullPath, []byte("boo"), 0600)
				Expect(err).ToNot(HaveOccurred())
				// end setup

				err = vfs.WriteFileWithPermissions(fullPath, []byte("something"), 0600)
				Expect(err).ToNot(HaveOccurred())

				fileInfo, err := testFs.Stat(dirPath)
				Expect(fileInfo).ToNot(BeNil())
				Expect(err).ToNot(HaveOccurred())

				tmpPath := fmt.Sprintf("%s.tmp", fullPath)
				fileInfo, err = testFs.Stat(tmpPath)
				Expect(fileInfo).To(BeNil())
				Expect(err).To(HaveOccurred())

				oldPath := fmt.Sprintf("%s.old", fullPath)
				fileInfo, err = testFs.Stat(oldPath)
				Expect(fileInfo).To(BeNil())
				Expect(err).To(HaveOccurred())
			})
		})

		When("chmod fails to change the permission", func() {
			It("returns an error", func() {
				testErr := errors.New("chmod failed")
				vfs.Dependency.Chmod = func(name string, mode os.FileMode) error {
					return testErr
				}
				expectedErrorCode = error.FileSystemIssue
				expectedErrorArgs = []any{testErr}

				err := vfs.WriteFileWithPermissions("/tmp/config.yml", []byte("something"), 0600)

				Expect(err).To(MatchError(expectedError))
				Expect(newErrorCallCount).To(Equal(1))
			})
		})
	})
})
