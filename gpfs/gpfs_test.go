package gpfs_test

import (
	"errors"
	"io"
	"os"
	"os/user"
	"path/filepath"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	vfs "github.com/spf13/afero"

	"github.com/greenplum-db/gp-common-go-libs/gpfs"
	"github.com/greenplum-db/gp-common-go-libs/gpfs/gpfsfakes"
)

var _ = Describe("gpfs", func() {
	var (
		testFilesystem *gpfsfakes.FakeFilesystem
	)

	BeforeEach(func() {
		gpfs.Default()
		testFilesystem = &gpfsfakes.FakeFilesystem{}
	})

	Context("GpFsImpl", func() {
		var (
			target gpfs.GpFs
		)

		BeforeEach(func() {
			target = gpfs.GpFsImpl{Fs: testFilesystem}
		})

		Context("Append", func() {
			var (
				testFile *gpfsfakes.FakeGpvFile
			)

			BeforeEach(func() {
				testFile = &gpfsfakes.FakeGpvFile{}

				gpfs.Dependency.CreateDir = func(fs vfs.Fs, fullPath string) error {
					Expect(fs).To(Equal(testFilesystem))
					Expect(fullPath).To(Equal("test-path"))
					return nil
				}
				gpfs.Dependency.OpenFile = func(fs vfs.Fs, path string, flags int, perms os.FileMode) (gpfs.GpvFile, error) {
					Expect(fs).To(Equal(testFilesystem))
					Expect(path).To(Equal("test-path/test-file"))
					Expect(flags).To(Equal(os.O_APPEND | os.O_WRONLY | os.O_CREATE))
					Expect(perms).To(Equal(os.FileMode(0765)))
					return testFile, nil
				}
				testFile.WriteStringReturns(4523, nil)
			})

			When("ensuring that a specified parent directory exists fails", func() {
				It("returns an error", func() {
					gpfs.Dependency.CreateDir = func(_ vfs.Fs, _ string) error {
						return errors.New("failed to ensure parent")
					}

					_, err := target.Append("test-path/test-file", "test-content", 0765)

					Expect(err).To(MatchError("failed to ensure parent"))
				})
			})

			When("opening the file fails", func() {
				It("returns an error", func() {
					gpfs.Dependency.OpenFile = func(_ vfs.Fs, _ string, _ int, _ os.FileMode) (gpfs.GpvFile, error) {
						return nil, errors.New("failed to open")
					}

					_, err := target.Append("test-path/test-file", "test-content", 0765)

					Expect(err).To(MatchError(`writing "test-path/test-file" failed due to: failed to open`))
					Expect(testFile.CloseCallCount()).To(Equal(0))
				})
			})

			When("writing to the file fails", func() {
				It("returns an error", func() {
					testFile.WriteStringReturns(0, errors.New("failed to write"))

					_, err := target.Append("test-path/test-file", "test-content", 0765)

					Expect(err).To(MatchError(`writing "test-path/test-file" failed due to: failed to write`))
					Expect(testFile.CloseCallCount()).To(Equal(1))
				})
			})

			When("writing to the file succeeds", func() {
				It("returns no error", func() {
					count, err := target.Append("test-path/test-file", "test-content", 0765)

					Expect(err).ToNot(HaveOccurred())
					Expect(count).To(Equal(4523))
					Expect(testFile.CloseCallCount()).To(Equal(1))
					Expect(testFile.WriteStringCallCount()).To(Equal(1))
					Expect(testFile.WriteStringArgsForCall(0)).To(Equal("test-content"))
				})
			})
		})

		Context("CheckExists", func() {
			var (
				testFile *gpfsfakes.FakeOsFile
			)

			BeforeEach(func() {
				testFile = &gpfsfakes.FakeOsFile{}
				testFilesystem.StatReturns(testFile, nil)
			})

			When("the file does not exist", func() {
				It("returns false and no error", func() {
					testFilesystem.StatReturns(nil, os.ErrNotExist)

					exists, err := target.CheckExists("test-path", gpfs.IsDir)

					Expect(err).ToNot(HaveOccurred())
					Expect(exists).To(BeFalse())
					Expect(testFilesystem.StatCallCount()).To(Equal(1))
					Expect(testFilesystem.StatArgsForCall(0)).To(Equal("test-path"))
				})
			})

			When("fetching a filesystem object fails for reasons other than its absence", func() {
				It("returns an error", func() {
					testFilesystem.StatReturns(nil, errors.New("failed to stat"))

					_, err := target.CheckExists("test-path", gpfs.IsDir)

					Expect(err).To(MatchError(`getting system info for "test-path" failed due to: failed to stat`))
				})
			})

			When("fetching a non-directory succeeds but a directory is expected", func() {
				It("returns an error", func() {
					_, err := target.CheckExists("test-path", gpfs.IsDir)

					Expect(err).To(MatchError(`expected directory "test-path" but found a non-directory instead`))
				})
			})

			When("fetching a directory succeeds but a non-directory is expected", func() {
				It("returns an error", func() {
					testFile.IsDirReturns(true)

					_, err := target.CheckExists("test-path", gpfs.IsFile)

					Expect(err).To(MatchError(`expected non-directory "test-path" but found a directory instead`))
				})
			})

			When("the target path exists and is of the expected type", func() {
				It("returns true and no error", func() {
					exists, err := target.CheckExists("test-path", gpfs.IsFile)

					Expect(err).ToNot(HaveOccurred())
					Expect(exists).To(BeTrue())
					Expect(testFilesystem.StatCallCount()).To(Equal(1))
					Expect(testFilesystem.StatArgsForCall(0)).To(Equal("test-path"))
				})
			})
		})

		Context("Chmod", func() {
			When("changing the permissions on a filesystem object fails", func() {
				It("returns any error which has been encountered", func() {
					gpfs.Dependency.Chmod = func(fs vfs.Fs, path string, perms os.FileMode) error {
						Expect(fs).To(Equal(testFilesystem))
						Expect(path).To(Equal("test-file-path"))
						Expect(perms).To(Equal(os.FileMode(0123)))
						return errors.New("failed to change perms")
					}

					Expect(target.Chmod("test-file-path", 0123)).To(MatchError("failed to change perms"))
				})
			})
		})

		Context("Chown", func() {
			When("changing ownership", func() {
				It("returns any error which has been encountered", func() {
					gpfs.Dependency.Chown = func(fs vfs.Fs, fullPath string, user string, group string) error {
						Expect(fs).To(Equal(testFilesystem))
						Expect(fullPath).To(Equal("test-path"))
						Expect(user).To(Equal("test-user"))
						Expect(group).To(Equal("test-group"))
						return errors.New("failed to chown")
					}

					Expect(target.Chown("test-path", "test-user", "test-group")).To(MatchError("failed to chown"))
				})
			})
		})

		Context("Copy", func() {
			var (
				testDestFile   *gpfsfakes.FakeGpvFile
				testSourceFile *gpfsfakes.FakeGpvFile
			)

			BeforeEach(func() {
				testSourceFile = &gpfsfakes.FakeGpvFile{}
				testDestFile = &gpfsfakes.FakeGpvFile{}

				gpfs.Dependency.OpenSourceFile = func(fs vfs.Fs, path string, flags int, perms os.FileMode) (gpfs.GpvFile, error) {
					Expect(fs).To(Equal(testFilesystem))
					Expect(path).To(Equal("test-source"))
					Expect(flags).To(Equal(os.O_RDONLY))
					Expect(perms).To(Equal(os.FileMode(0753)))
					return testSourceFile, nil
				}
				gpfs.Dependency.OpenDestFile = func(fs vfs.Fs, path string, flags int, perms os.FileMode) (gpfs.GpvFile, error) {
					Expect(fs).To(Equal(testFilesystem))
					Expect(path).To(Equal("test-dest"))
					Expect(flags).To(Equal(os.O_WRONLY | os.O_CREATE))
					Expect(perms).To(Equal(os.FileMode(0753)))
					return testDestFile, nil
				}
				gpfs.Dependency.Copy = func(dest io.Writer, source io.Reader) (int64, error) {
					Expect(dest).To(Equal(testDestFile))
					Expect(source).To(Equal(testSourceFile))
					return 5432, nil
				}
			})

			When("opening the source file fails", func() {
				It("returns an error", func() {
					gpfs.Dependency.OpenSourceFile = func(_ vfs.Fs, _ string, _ int, _ os.FileMode) (gpfs.GpvFile, error) {
						return nil, errors.New("failed to open source")
					}

					Expect(target.Copy("test-source", "test-dest", 0753)).To(MatchError("failed to open source"))
					Expect(testSourceFile.CloseCallCount()).To(Equal(0))
					Expect(testDestFile.CloseCallCount()).To(Equal(0))
				})
			})

			When("opening the destination file fails", func() {
				It("returns an error", func() {
					gpfs.Dependency.OpenDestFile = func(_ vfs.Fs, _ string, _ int, _ os.FileMode) (gpfs.GpvFile, error) {
						return nil, errors.New("failed to open dest")
					}

					Expect(target.Copy("test-source", "test-dest", 0753)).To(MatchError("failed to open dest"))
					Expect(testSourceFile.CloseCallCount()).To(Equal(1))
					Expect(testDestFile.CloseCallCount()).To(Equal(0))
				})
			})

			When("copying the source file's content file fails", func() {
				It("returns an error", func() {
					gpfs.Dependency.Copy = func(dest io.Writer, source io.Reader) (int64, error) {
						return -1, errors.New("failed to copy")
					}

					Expect(target.Copy("test-source", "test-dest", 0753)).To(MatchError("failed to copy"))
					Expect(testSourceFile.CloseCallCount()).To(Equal(1))
					Expect(testDestFile.CloseCallCount()).To(Equal(1))
				})
			})

			When("copying the source file's content file succeeds", func() {
				It("returns no error", func() {
					Expect(target.Copy("test-source", "test-dest", 0753)).To(Succeed())
					Expect(testSourceFile.CloseCallCount()).To(Equal(1))
					Expect(testDestFile.CloseCallCount()).To(Equal(1))
				})
			})
		})

		Context("CreateDir", func() {
			When("creating a full directory path", func() {
				It("returns any error which is encountered", func() {
					gpfs.Dependency.CreateDir = func(fs vfs.Fs, fullPath string) error {
						Expect(fs).To(Equal(testFilesystem))
						Expect(fullPath).To(Equal("test-file-path"))
						return errors.New("failed to create")
					}

					Expect(target.CreateDir("test-file-path")).To(MatchError("failed to create"))
				})
			})
		})

		Context("Delete", func() {
			When("removing a filesystem object fails", func() {
				It("returns an error", func() {
					testFilesystem.RemoveAllReturns(errors.New("failed to remove"))

					err := target.Delete("test-file-path")

					Expect(err).To(MatchError(`removing "test-file-path" failed due to: failed to remove`))
				})
			})

			When("removing a filesystem object succeeds", func() {
				It("returns no error", func() {
					Expect(target.Delete("test-file-path")).To(Succeed())
					Expect(testFilesystem.RemoveAllCallCount()).To(Equal(1))
					Expect(testFilesystem.RemoveAllArgsForCall(0)).To(Equal("test-file-path"))
				})
			})
		})

		Context("GetFileMode", func() {
			var (
				testOsFile *gpfsfakes.FakeOsFile
			)

			BeforeEach(func() {
				testOsFile = &gpfsfakes.FakeOsFile{}
				testOsFile.ModeReturns(os.FileMode(07345))
				testFilesystem.StatReturns(testOsFile, nil)
				gpfs.Dependency.Stat = func(fs vfs.Fs, path string) (gpfs.OsFile, error) {
					Expect(fs).To(Equal(testFilesystem))
					Expect(path).To(Equal("test-path"))
					return testOsFile, nil
				}
			})

			When("fetching filesystem information for a file fails", func() {
				It("returns an error", func() {
					gpfs.Dependency.Stat = func(_ vfs.Fs, _ string) (gpfs.OsFile, error) {
						return nil, errors.New("failed to stat")
					}

					_, err := target.GetFileMode("test-path")

					Expect(err).To(MatchError("failed to stat"))
				})
			})

			When("fetching filesystem information for a file succeeds", func() {
				It("returns the permissions and no error", func() {
					perms, err := target.GetFileMode("test-path")

					Expect(err).ToNot(HaveOccurred())
					Expect(perms).To(Equal(os.FileMode(0345)))
				})
			})
		})

		Context("GetUidAndGidForPath", func() {
			var (
				testOsFile *gpfsfakes.FakeOsFile
			)

			BeforeEach(func() {
				testOsFile = &gpfsfakes.FakeOsFile{}
				gpfs.Dependency.Stat = func(fs vfs.Fs, path string) (gpfs.OsFile, error) {
					Expect(fs).To(Equal(testFilesystem))
					Expect(path).To(Equal("test-path"))
					return testOsFile, nil
				}
				gpfs.Dependency.GetUidAndGidFromFile = func(fileInfo gpfs.OsFile) (int, int) {
					Expect(fileInfo).To(Equal(testOsFile))
					return 1234, 5678
				}
			})

			When("fetching filesystem information for a file fails", func() {
				It("returns an error", func() {
					gpfs.Dependency.Stat = func(_ vfs.Fs, _ string) (gpfs.OsFile, error) {
						return nil, errors.New("failed to stat")
					}

					_, _, err := target.GetUidAndGidForPath("test-path")

					Expect(err).To(MatchError("failed to stat"))
				})
			})

			When("fetching the file info succeeds", func() {
				It("returns the file's UID and GID, and no error", func() {
					uid, gid, err := target.GetUidAndGidForPath("test-path")

					Expect(err).ToNot(HaveOccurred())
					Expect(uid).To(Equal(1234))
					Expect(gid).To(Equal(5678))
				})
			})
		})

		Context("Glob", func() {
			BeforeEach(func() {
				gpfs.Dependency.Glob = func(fs vfs.Fs, pattern string) ([]string, error) {
					Expect(fs).To(Equal(testFilesystem))
					Expect(pattern).To(Equal("test-pattern"))
					return []string{"test-1", "test-2"}, nil
				}
			})

			When("matching a pattern for the names of filesystem objects fails", func() {
				It("returns an error", func() {
					gpfs.Dependency.Glob = func(_ vfs.Fs, _ string) ([]string, error) {
						return nil, errors.New("failed to glob")
					}

					_, err := target.Glob("test-pattern")

					Expect(err).To(MatchError(`failed to match "test-pattern" due to: failed to glob`))
				})
			})

			When("matching a pattern for the names of filesystem objects succeeds", func() {
				It("returns the matches and no error", func() {
					matches, err := target.Glob("test-pattern")

					Expect(err).ToNot(HaveOccurred())
					Expect(matches).To(Equal([]string{"test-1", "test-2"}))
				})
			})
		})

		Context("MatchLines", func() {
			When("attempting to match lines in a file", func() {
				It("returns any matches which are discovered, as well as any error which is encountered", func() {
					gpfs.Dependency.MatchLines = func(fs vfs.Fs, fullPath string, pattern string) ([]string, error) {
						Expect(fs).To(Equal(testFilesystem))
						Expect(fullPath).To(Equal("test-path"))
						Expect(pattern).To(Equal("test-pattern"))
						return []string{"test-match-1", "test-match-2"}, errors.New("failed to match")
					}

					matches, err := target.MatchPattern("test-path", "test-pattern")
					Expect(err).To(MatchError("failed to match"))
					Expect(matches).To(Equal([]string{"test-match-1", "test-match-2"}))
				})
			})
		})

		Context("MatchString", func() {
			When("attempting to match lines as exact strings in a file", func() {
				It("returns any matches which are discovered, as well as any error which is encountered", func() {
					gpfs.Dependency.MatchLines = func(fs vfs.Fs, fullPath string, pattern string) ([]string, error) {
						Expect(fs).To(Equal(testFilesystem))
						Expect(fullPath).To(Equal("test-path"))
						Expect(pattern).To(Equal("^test-string$"))
						return []string{"test-match-1", "test-match-2"}, errors.New("failed to match")
					}

					matches, err := target.MatchString("test-path", "test-string")
					Expect(err).To(MatchError("failed to match"))
					Expect(matches).To(Equal([]string{"test-match-1", "test-match-2"}))
				})
			})
		})

		Context("Read", func() {
			BeforeEach(func() {
				gpfs.Dependency.ReadFile = func(fs vfs.Fs, path string) ([]byte, error) {
					Expect(fs).To(Equal(testFilesystem))
					Expect(path).To(Equal("test-path"))
					return []byte("test-content"), nil
				}
			})

			When("reading the contents of a file fails", func() {
				It("returns an error", func() {
					gpfs.Dependency.ReadFile = func(_ vfs.Fs, _ string) ([]byte, error) {
						return nil, errors.New("failed to read")
					}

					_, err := target.Read("test-path")

					Expect(err).To(MatchError(`failed to read "test-path" due to: failed to read`))
				})
			})

			When("reading the contents of a file succeeds", func() {
				It("returns the contents and no error", func() {
					content, err := target.Read("test-path")

					Expect(err).ToNot(HaveOccurred())
					Expect(content).To(Equal("test-content"))
				})
			})
		})

		Context("RecursiveChown", func() {
			When("it attempts to traverse the filesystem from the specified path and apply changes of owner", func() {
				It("returns any error which it encounters", func() {
					testOsFile := &gpfsfakes.FakeOsFile{}
					gpfs.Dependency.GetChownWalkFunc = func(fs vfs.Fs, user string, group string) filepath.WalkFunc {
						Expect(fs).To(Equal(testFilesystem))
						Expect(user).To(Equal("test-user"))
						Expect(group).To(Equal("test-group"))
						return func(filename string, fileInfo os.FileInfo, err error) error {
							Expect(filename).To(Equal("test-filename"))
							Expect(fileInfo).To(Equal(testOsFile))
							Expect(err).To(MatchError("test-input-error"))
							return errors.New("test-jackpot-error")
						}
					}
					gpfs.Dependency.Walk = func(fs vfs.Fs, path string, walkFn filepath.WalkFunc) error {
						Expect(fs).To(Equal(testFilesystem))
						Expect(path).To(Equal("test-path"))
						Expect(walkFn("test-filename", testOsFile, errors.New("test-input-error"))).To(MatchError("test-jackpot-error"))
						return errors.New("failed to walk")
					}

					err := target.RecursiveChown("test-path", "test-user", "test-group")

					Expect(err).To(MatchError("failed to walk"))
				})
			})
		})

		Context("Write", func() {
			BeforeEach(func() {
				gpfs.Dependency.CreateDir = func(fs vfs.Fs, fullPath string) error {
					Expect(fs).To(Equal(testFilesystem))
					Expect(fullPath).To(Equal("test-path"))
					return nil
				}
				gpfs.Dependency.WriteFile = func(fs vfs.Fs, path string, content []byte, perms os.FileMode) error {
					Expect(fs).To(Equal(testFilesystem))
					Expect(path).To(Equal("test-path/test-file"))
					Expect(string(content)).To(Equal("test-content"))
					Expect(perms).To(Equal(os.FileMode(0123)))
					return nil
				}
				gpfs.Dependency.Chmod = func(fs vfs.Fs, path string, perms os.FileMode) error {
					Expect(fs).To(Equal(testFilesystem))
					Expect(path).To(Equal("test-path/test-file"))
					Expect(perms).To(Equal(os.FileMode(0123)))
					return nil
				}
			})

			When("ensuring that a specified parent directory exists fails", func() {
				It("returns an error", func() {
					gpfs.Dependency.CreateDir = func(_ vfs.Fs, _ string) error {
						return errors.New("failed to ensure parent")
					}

					Expect(target.Write("test-path/test-file", "test-content", 0123)).To(MatchError("failed to ensure parent"))
				})
			})

			When("reading the contents of a file fails", func() {
				It("returns an error", func() {
					gpfs.Dependency.WriteFile = func(_ vfs.Fs, _ string, _ []byte, _ os.FileMode) error {
						return errors.New("failed to write")
					}

					err := target.Write("test-path/test-file", "test-content", 0123)

					Expect(err).To(MatchError(`failed to write "test-path/test-file" due to: failed to write`))
				})
			})

			When("changing the permissions of a file fails", func() {
				It("returns an error", func() {
					gpfs.Dependency.Chmod = func(_ vfs.Fs, _ string, _ os.FileMode) error {
						return errors.New("failed to change perms")
					}

					err := target.Write("test-path/test-file", "test-content", 0123)

					Expect(err).To(MatchError("failed to change perms"))
				})
			})

			When("reading the contents of a file succeeds", func() {
				It("returns the contents and no error", func() {
					Expect(target.Write("test-path/test-file", "test-content", 0123)).ToNot(HaveOccurred())
				})
			})
		})
	})

	Context("package-level functions", func() {
		Context("Chmod", func() {
			When("changing the permissions on a filesystem object fails", func() {
				It("returns an error", func() {
					testFilesystem.ChmodReturns(errors.New("failed to change perms"))

					err := gpfs.Chmod(testFilesystem, "test-file-path", 0111)

					Expect(err).To(MatchError(`changing permissions for "test-file-path" failed due to: failed to change perms`))
				})
			})

			When("changing the permissions on a filesystem object succeeds", func() {
				It("returns no error", func() {
					Expect(gpfs.Chmod(testFilesystem, "test-file-path", 0111)).To(Succeed())
					Expect(testFilesystem.ChmodCallCount()).To(Equal(1))
					path, perms := testFilesystem.ChmodArgsForCall(0)
					Expect(path).To(Equal("test-file-path"))
					Expect(perms).To(Equal(os.FileMode(0111)))
				})
			})
		})

		Context("Chown", func() {
			BeforeEach(func() {
				gpfs.Dependency.GetUid = func(username string) (int, error) {
					Expect(username).To(Equal("test-user"))
					return 101, nil
				}
				gpfs.Dependency.GetGid = func(groupname string) (int, error) {
					Expect(groupname).To(Equal("test-group"))
					return 201, nil
				}
			})

			When("fetching the UID for a username fails", func() {
				It("returns an error", func() {
					gpfs.Dependency.GetUid = func(_ string) (int, error) {
						return -1, errors.New("failed to get UID")
					}

					Expect(gpfs.Chown(testFilesystem, "test-path", "test-user", "test-group")).To(MatchError("failed to get UID"))
				})
			})

			When("fetching the GID for a group name fails", func() {
				It("returns an error", func() {
					gpfs.Dependency.GetGid = func(_ string) (int, error) {
						return -1, errors.New("failed to get GID")
					}

					Expect(gpfs.Chown(testFilesystem, "test-path", "test-user", "test-group")).To(MatchError("failed to get GID"))
				})
			})

			When("changing ownership fails", func() {
				It("returns an error", func() {
					testFilesystem.ChownReturns(errors.New("failed to change ownership"))

					err := gpfs.Chown(testFilesystem, "test-path", "test-user", "test-group")

					Expect(err).To(MatchError(`changing ownership of "test-path" to "test-user:test-group" failed due to: failed to change ownership`))
				})
			})

			When("changing ownership succeeds", func() {
				It("returns no error", func() {
					Expect(gpfs.Chown(testFilesystem, "test-path", "test-user", "test-group")).To(Succeed())
					Expect(testFilesystem.ChownCallCount()).To(Equal(1))
					path, uid, gid := testFilesystem.ChownArgsForCall(0)
					Expect(path).To(Equal("test-path"))
					Expect(uid).To(Equal(101))
					Expect(gid).To(Equal(201))
				})
			})
		})

		Context("CreateDir", func() {
			When("creating a full directory path fails", func() {
				It("returns an error", func() {
					testFilesystem.MkdirAllReturns(errors.New("failed to create"))

					err := gpfs.CreateDir(testFilesystem, "test-file-path")

					Expect(err).To(MatchError(`creating "test-file-path" failed due to: failed to create`))
				})
			})

			When("removing a filesystem object succeeds", func() {
				It("returns no error", func() {
					Expect(gpfs.CreateDir(testFilesystem, "test-file-path")).To(Succeed())
					Expect(testFilesystem.MkdirAllCallCount()).To(Equal(1))
					path, perms := testFilesystem.MkdirAllArgsForCall(0)
					Expect(path).To(Equal("test-file-path"))
					Expect(perms).To(Equal(os.FileMode(0755)))
				})
			})
		})

		Context("GetChownWalkFunc", func() {
			var (
				chownCallCount int
			)

			BeforeEach(func() {
				chownCallCount = 0
				gpfs.Dependency.Chown = func(fs vfs.Fs, fullPath string, user string, group string) error {
					Expect(fs).To(Equal(testFilesystem))
					Expect(fullPath).To(Equal("test-file"))
					Expect(user).To(Equal("test-user"))
					Expect(group).To(Equal("test-group"))
					chownCallCount++
					return nil
				}
			})

			When("an error is passed into the walk function", func() {
				It("returns the error without changing ownership", func() {
					f := gpfs.GetChownWalkFunc(testFilesystem, "test-user", "test-group")

					Expect(f("test-file", nil, errors.New("test-input-error"))).To(MatchError("test-input-error"))
					Expect(chownCallCount).To(Equal(0))
				})
			})

			When("an error is encountered while changing ownership of a filesystem object", func() {
				It("returns the error", func() {
					gpfs.Dependency.Chown = func(_ vfs.Fs, _ string, _ string, _ string) error {
						return errors.New("failed to chown")
					}

					f := gpfs.GetChownWalkFunc(testFilesystem, "test-user", "test-group")

					Expect(f("test-file", nil, nil)).To(MatchError("failed to chown"))
					Expect(chownCallCount).To(Equal(0))
				})
			})

			When("no errors are encountered", func() {
				It("returns no error", func() {
					f := gpfs.GetChownWalkFunc(testFilesystem, "test-user", "test-group")

					Expect(f("test-file", nil, nil)).To(Succeed())
					Expect(chownCallCount).To(Equal(1))
				})
			})
		})

		Context("GetGid", func() {
			var (
				testGroup *user.Group
			)

			BeforeEach(func() {
				testGroup = &user.Group{
					Gid: "8675309",
				}
				gpfs.Dependency.LookupGroup = func(groupname string) (*user.Group, error) {
					Expect(groupname).To(Equal("test-group"))
					return testGroup, nil
				}
			})

			When("fetching a user.Group by group name fails", func() {
				It("returns an error", func() {
					gpfs.Dependency.LookupGroup = func(_ string) (*user.Group, error) {
						return nil, errors.New("unable to lookup group")
					}

					_, err := gpfs.GetGid("test-group")

					Expect(err).To(MatchError(`fetching group ID for "test-group" failed due to: unable to lookup group`))
				})
			})

			When("converting the GID string into an int fails", func() {
				It("returns an error", func() {
					testGroup.Gid = "867XYZ9"

					_, err := gpfs.GetGid("test-group")

					Expect(err).To(MatchError(`unable to convert group ID "867XYZ9" into an integer`))
				})
			})

			When("fetching a user.Group by username succeeds", func() {
				It("returns the user's GID and no error", func() {
					gid, err := gpfs.GetGid("test-group")

					Expect(err).ToNot(HaveOccurred())
					Expect(gid).To(Equal(8675309))
				})
			})
		})

		Context("GetHomeDir", func() {
			var (
				testUser *user.User
			)

			BeforeEach(func() {
				testUser = &user.User{
					HomeDir: "test-home-dir",
				}

				gpfs.Dependency.LookupUser = func(username string) (*user.User, error) {
					Expect(username).To(Equal("test-user"))
					return testUser, nil
				}
			})

			When("looking-up a user fails", func() {
				It("returns an error", func() {
					gpfs.Dependency.LookupUser = func(_ string) (*user.User, error) {
						return nil, errors.New("unable to lookup user")
					}

					_, err := gpfs.GetHomeDir("test-user")

					Expect(err).To(MatchError("unable to lookup user"))
				})
			})

			When("looking-up a user succeeds", func() {
				It("returns the user's home directory and no error", func() {
					homeDir, err := gpfs.GetHomeDir("test-user")

					Expect(err).ToNot(HaveOccurred())
					Expect(homeDir).To(Equal("test-home-dir"))
				})
			})
		})

		Context("GetMatches", func() {
			When("provided with a file reference and a regular expression matcher", func() {
				It("returns the lines which match the expression", func() {
					testFile := &gpfsfakes.FakeGpvFile{}
					testRegexp := &gpfsfakes.FakeGpvRegexp{}
					testScanner := &gpfsfakes.FakeGpvScanner{}

					testScanner.ScanReturns(true)
					testScanner.ScanReturnsOnCall(4, false)
					testScanner.TextReturns("test-unmatched")
					testScanner.TextReturnsOnCall(0, "test-line-1")
					testScanner.TextReturnsOnCall(1, "test-line-2")
					testScanner.TextReturnsOnCall(2, "test-line-3")
					testScanner.TextReturnsOnCall(3, "test-line-4")
					testScanner.TextReturnsOnCall(4, "test-line-5")

					testRegexp.MatchStringReturnsOnCall(1, true)
					testRegexp.MatchStringReturnsOnCall(3, true)
					testRegexp.MatchStringReturnsOnCall(4, true)

					gpfs.Dependency.GetScanner = func(f gpfs.GpvFile) gpfs.GpvScanner {
						Expect(f).To(Equal(testFile))
						return testScanner
					}

					Expect(gpfs.GetMatches(testFile, testRegexp)).To(Equal([]string{"test-line-2", "test-line-4"}))
				})
			})
		})

		Context("GetUid", func() {
			var (
				testUser *user.User
			)

			BeforeEach(func() {
				testUser = &user.User{
					Uid: "90210",
				}

				gpfs.Dependency.LookupUser = func(username string) (*user.User, error) {
					Expect(username).To(Equal("test-user"))
					return testUser, nil
				}
			})

			When("fetching a user.User by username fails", func() {
				It("returns an error", func() {
					gpfs.Dependency.LookupUser = func(_ string) (*user.User, error) {
						return nil, errors.New("user not found")
					}

					_, err := gpfs.GetUid("test-user")

					Expect(err).To(MatchError(`unable to lookup user "test-user" failed due to: user not found`))
				})
			})

			When("converting the UID string into an int fails", func() {
				It("returns an error", func() {
					testUser.Uid = "[6.X,Z9"

					_, err := gpfs.GetUid("test-user")

					Expect(err).To(MatchError(`unable to convert user ID "[6.X,Z9" into an integer`))
				})
			})

			When("fetching a user.User by username succeeds", func() {
				It("returns the user's UID and no error", func() {
					uid, err := gpfs.GetUid("test-user")

					Expect(err).ToNot(HaveOccurred())
					Expect(uid).To(Equal(90210))
				})
			})
		})

		Context("MatchLines", func() {
			var (
				testFile   *gpfsfakes.FakeGpvFile
				testRegexp *gpfsfakes.FakeGpvRegexp
			)

			BeforeEach(func() {
				testRegexp = &gpfsfakes.FakeGpvRegexp{}
				testFile = &gpfsfakes.FakeGpvFile{}

				gpfs.Dependency.RegexpCompile = func(pattern string) (gpfs.GpvRegexp, error) {
					Expect(pattern).To(Equal("test-pattern"))
					return testRegexp, nil
				}
				gpfs.Dependency.OpenFile = func(fs vfs.Fs, path string, flags int, perms os.FileMode) (gpfs.GpvFile, error) {
					Expect(fs).To(Equal(testFilesystem))
					Expect(path).To(Equal("test-path"))
					Expect(flags).To(Equal(os.O_RDONLY))
					Expect(perms).To(Equal(os.FileMode(0600)))
					return testFile, nil
				}
				gpfs.Dependency.GetMatches = func(f gpfs.GpvFile, r gpfs.GpvRegexp) []string {
					Expect(f).To(Equal(testFile))
					Expect(r).To(Equal(testRegexp))
					return []string{"test-match-1", "test-match-2"}
				}
			})

			When("compiling the regular expression matcher fails", func() {
				It("returns an error", func() {
					gpfs.Dependency.RegexpCompile = func(_ string) (gpfs.GpvRegexp, error) {
						return nil, errors.New("failed to compile regexp")
					}

					_, err := gpfs.MatchLines(testFilesystem, "test-path", "test-pattern")

					Expect(err).To(MatchError("failed to compile regexp"))
				})
			})

			When("opening the file fails", func() {
				It("returns an error", func() {
					gpfs.Dependency.OpenFile = func(_ vfs.Fs, _ string, _ int, _ os.FileMode) (gpfs.GpvFile, error) {
						return nil, errors.New("failed to open")
					}

					_, err := gpfs.MatchLines(testFilesystem, "test-path", "test-pattern")

					Expect(err).To(MatchError("failed to open"))
				})
			})

			When("matching lines succeeds", func() {
				It("returns the matches and no error", func() {
					matches, err := gpfs.MatchLines(testFilesystem, "test-path", "test-pattern")

					Expect(err).ToNot(HaveOccurred())
					Expect(matches).To(Equal([]string{"test-match-1", "test-match-2"}))
				})
			})
		})

		Context("OpenFile", func() {
			var (
				testGpvFile *gpfsfakes.FakeGpvFile
			)

			BeforeEach(func() {
				testGpvFile = &gpfsfakes.FakeGpvFile{}
				testFilesystem.OpenFileReturns(testGpvFile, nil)
			})

			When("opening the file fails", func() {
				It("returns an error", func() {
					testFilesystem.OpenFileReturns(nil, errors.New("failed to open"))

					_, err := gpfs.OpenFile(testFilesystem, "test-file-path", 5432, 0765)

					Expect(err).To(MatchError(`opening "test-file-path" failed due to: failed to open`))
				})
			})

			When("opening the file succeeds", func() {
				It("returns a file handle and no error", func() {
					f, err := gpfs.OpenFile(testFilesystem, "test-file-path", 5432, 0765)

					Expect(err).ToNot(HaveOccurred())
					Expect(f).To(Equal(testGpvFile))
					Expect(testFilesystem.OpenFileCallCount()).To(Equal(1))
					path, flags, perms := testFilesystem.OpenFileArgsForCall(0)
					Expect(path).To(Equal("test-file-path"))
					Expect(flags).To(Equal(5432))
					Expect(perms).To(Equal(os.FileMode(0765)))
				})
			})
		})

		Context("Stat", func() {
			var (
				testFileInfo *gpfsfakes.FakeOsFile
			)

			BeforeEach(func() {
				testFileInfo = &gpfsfakes.FakeOsFile{}
				testFilesystem.StatReturns(testFileInfo, nil)
			})

			When("fetching filesystem information for a file fails", func() {
				It("returns an error", func() {
					testFilesystem.StatReturns(nil, errors.New("failed to stat"))

					_, err := gpfs.Stat(testFilesystem, "test-path")

					Expect(err).To(MatchError(`failed to fetch file permissions for "test-path" due to: failed to stat`))
				})
			})

			When("fetching a file's info succeeds", func() {
				It("returns the file's info and no error", func() {
					fileInfo, err := gpfs.Stat(testFilesystem, "test-path")

					Expect(err).ToNot(HaveOccurred())
					Expect(fileInfo).To(Equal(testFileInfo))
				})
			})
		})
	})
})
