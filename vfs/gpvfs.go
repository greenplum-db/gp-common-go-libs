package vfs

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path"

	"github.com/blang/vfs"
	"github.com/blang/vfs/memfs"

	"github.com/greenplum-db/gp-common-go-libs/constants"
	error "github.com/greenplum-db/gp-common-go-libs/error"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

//counterfeiter:generate . GpvFilesystem
type GpvFilesystem vfs.Filesystem

//counterfeiter:generate . GpvFile
type GpvFile vfs.File

var fs GpvFilesystem

func init() {
	SetFS()
	Default()
}

var Dependency struct {
	Chmod                       func(name string, mode os.FileMode) error
	Create                      func(name string) (vfs.File, error)
	EnsureParentDirectoryExists func(fullPath string) error
	GetFs                       func() GpvFilesystem
	GetSha256Hash               func() ShaHash
	HexEncodeToString           func(src []byte) string
	MkdirAll                    func(path string, perm os.FileMode) error
	NewError                    func(errorCode constants.ErrorCode, args ...any) error.Error
	OpenFile                    func(fullPath string, flag int, perm os.FileMode) (GpvFile, error)
	OsChmod                     func(name string, mode os.FileMode) error
	ReadFilePrivate             func(fs vfs.Filesystem, filename string) ([]byte, error)
	Remove                      func(name string)
	RenameToOld                 func(currentPath string, currentPathDotOld string) error
	RenameFromTmp               func(currentPathDotTmp string, currentPath string) error
	Stat                        func(path string) (os.FileInfo, error)
	StreamCopy                  func(dst io.Writer, src io.Reader) (written int64, err error)
	WriteFileWithPermissions    func(fullPath string, data []byte, mode os.FileMode) error
	WriteVfsFile                func(filename string, data []byte, perm os.FileMode) error
}

func Default() {
	Dependency.Chmod = Chmod
	Dependency.Create = create
	Dependency.EnsureParentDirectoryExists = EnsureParentDirectoryExists
	Dependency.GetFs = GetFs
	Dependency.GetSha256Hash = getSha256Hash
	Dependency.HexEncodeToString = hex.EncodeToString
	Dependency.MkdirAll = mkdirAll
	Dependency.NewError = error.New
	Dependency.OpenFile = OpenFile
	Dependency.OsChmod = os.Chmod
	Dependency.ReadFilePrivate = readFilePrivate
	Dependency.Remove = remove
	Dependency.RenameToOld = rename
	Dependency.RenameFromTmp = rename
	Dependency.Stat = Stat
	Dependency.StreamCopy = io.Copy
	Dependency.WriteFileWithPermissions = WriteFileWithPermissions
	Dependency.WriteVfsFile = writeVfsFile
}

func SetFS() GpvFilesystem {
	fs = vfs.OS()
	return fs
}

func SetTestFs() GpvFilesystem {
	fs = memfs.Create()
	return fs
}

func GetFs() GpvFilesystem {
	return fs
}

func AppendFile(fullPath string, data []byte) (int, error) {
	err := Dependency.EnsureParentDirectoryExists(fullPath)
	if err != nil {
		return 0, err
	}

	f, err := Dependency.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	byteCount, err := f.Write(data)
	if err != nil {
		return byteCount, Dependency.NewError(error.FileSystemIssue, err)
	}

	return byteCount, nil
}

func Chmod(filename string, mode os.FileMode) error {
	if _, ok := fs.(*memfs.MemFS); ok {
		return nil
	}
	return Dependency.OsChmod(filename, mode)
}

func EnsureParentDirectoryExists(fullPath string) error {
	folderPath := path.Dir(fullPath)
	if err := Dependency.MkdirAll(folderPath, 0700); err != nil {
		return Dependency.NewError(error.FileSystemIssue, err)
	}

	return nil
}

func GetChecksum(fullPath string) (string, error) {
	f, err := Dependency.OpenFile(fullPath, os.O_RDONLY, 0000)
	if err != nil {
		return "", err
	}
	defer f.Close()

	hash := Dependency.GetSha256Hash()

	if _, err := Dependency.StreamCopy(hash, f); err != nil {
		return "", Dependency.NewError(error.FailedToComputeChecksumForFile, fullPath, err)
	}

	checksum := hash.Sum(nil)

	return Dependency.HexEncodeToString(checksum), nil
}

func OpenFile(fullPath string, flags int, perms os.FileMode) (GpvFile, error) {
	file, err := Dependency.GetFs().OpenFile(fullPath, flags, perms)
	if err != nil {
		return nil, Dependency.NewError(error.FileSystemIssue, err)
	}

	return file, nil
}

func ReadFile(filePath string) ([]byte, error) {
	content, err := Dependency.ReadFilePrivate(fs, filePath)
	if err != nil {
		return nil, Dependency.NewError(error.FailedToReadFile, filePath, err)
	}

	return content, nil
}

func Stat(path string) (os.FileInfo, error) {
	// Because we may change the value of `fs` in a test, we need to wrap this function
	return fs.Stat(path)
}

func WriteFile(fullPath string, data []byte) error {
	return Dependency.WriteFileWithPermissions(fullPath, data, 0600)
}

func WriteFileWithPermissions(fullPath string, data []byte, mode os.FileMode) error {
	err := Dependency.EnsureParentDirectoryExists(fullPath)
	if err != nil {
		return err
	}

	filePathTmp := fmt.Sprintf("%s.tmp", fullPath)
	if err := Dependency.WriteVfsFile(filePathTmp, data, mode); err != nil {
		return Dependency.NewError(error.FileSystemIssue, err)
	}

	// If we don't use Chmod, the initial permission of a new file will be limited by `umask` on Linux.
	// See [https://man7.org/linux/man-pages/man2/umask.2.html]
	if err := Dependency.Chmod(filePathTmp, mode); err != nil {
		return Dependency.NewError(error.FileSystemIssue, err)
	}

	filePathOld := fmt.Sprintf("%s.old", fullPath)
	if _, err := Dependency.Stat(fullPath); err == nil {
		if err := Dependency.RenameToOld(fullPath, filePathOld); err != nil {
			return Dependency.NewError(error.FileSystemIssue, err)
		}
	}

	if err := Dependency.RenameFromTmp(filePathTmp, fullPath); err != nil {
		// Consider attempting a recovery of the old file
		return Dependency.NewError(error.FileSystemIssue, err)
	}

	Dependency.Remove(filePathOld)

	return nil
}

func create(filename string) (vfs.File, error) {
	return vfs.Create(fs, filename)
}

//counterfeiter:generate . ShaHash
type ShaHash hash.Hash

func getSha256Hash() ShaHash {
	return sha256.New()
}

func mkdirAll(folderPath string, perm os.FileMode) error {
	return vfs.MkdirAll(fs, folderPath, 0700)
}

func readFilePrivate(fs vfs.Filesystem, filename string) ([]byte, error) {
	return vfs.ReadFile(fs, filename)
}

func remove(filename string) {
	// Intentionally ignoring the result of the attempt to remove this temporary file
	// nolint:errcheck
	fs.Remove(filename)
}

func rename(oldPath string, newPath string) error {
	return fs.Rename(oldPath, newPath)
}

func writeVfsFile(filename string, data []byte, perm os.FileMode) error {
	return vfs.WriteFile(fs, filename, data, perm)
}
