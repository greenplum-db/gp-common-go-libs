package operating

/*
 * This file contains structs and functions used as entry points for
 * unit testing via dependency injection.
 */

import (
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"time"
)

var (
	System = InitializeSystemFunctions()
)

/*
 * Structs and functions for mocking out file reading
 */

type ReadCloserAt interface {
	io.ReadCloser
	io.ReaderAt
}

func OpenFileRead(name string, flag int, perm os.FileMode) (ReadCloserAt, error) {
	var reader ReadCloserAt
	var err error
	reader, err = os.OpenFile(name, flag, perm)
	return reader, err
}

func OpenFileWrite(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
	var writer io.WriteCloser
	var err error
	writer, err = os.OpenFile(name, flag, perm)
	return writer, err
}

/*
 * SystemFunctions holds function pointers for built-in functions that will need
 * to be mocked out for unit testing.  All built-in functions manipulating the
 * filesystem, shell, or environment should ideally be called through a function
 * pointer in System (the global SystemFunctions variable) instead of being called
 * directly.
 *
 * All function pointers in SystemFunctions refer directly to built-in functions
 * except for OpenFileRead and OpenFileWrite, which both refer to os.OpenFile but
 * return either an io.ReadCloser or io.WriteCloser instead of an *os.File, to make
 * mocking file opening in tests easier.
 */

type SystemFunctions struct {
	Chmod         func(name string, mode os.FileMode) error
	CurrentUser   func() (*user.User, error)
	Getenv        func(key string) string
	Getpid        func() int
	Glob          func(pattern string) (matches []string, err error)
	Hostname      func() (string, error)
	IsNotExist    func(err error) bool
	MkdirAll      func(path string, perm os.FileMode) error
	Now           func() time.Time
	OpenFileRead  func(name string, flag int, perm os.FileMode) (ReadCloserAt, error)
	OpenFileWrite func(name string, flag int, perm os.FileMode) (io.WriteCloser, error)
	ReadFile      func(filename string) ([]byte, error)
	Remove        func(name string) error
	RemoveAll     func(name string) error
	Stat          func(name string) (os.FileInfo, error)
	Stdin         ReadCloserAt
	Stdout        io.WriteCloser
	TempFile      func(dir, pattern string) (f *os.File, err error)
	Local         *time.Location
}

func InitializeSystemFunctions() *SystemFunctions {
	return &SystemFunctions{
		Chmod:         os.Chmod,
		CurrentUser:   user.Current,
		Getenv:        os.Getenv,
		Getpid:        os.Getpid,
		Glob:          filepath.Glob,
		Hostname:      os.Hostname,
		IsNotExist:    os.IsNotExist,
		MkdirAll:      os.MkdirAll,
		Now:           time.Now,
		OpenFileRead:  OpenFileRead,
		OpenFileWrite: OpenFileWrite,
		ReadFile:      ioutil.ReadFile,
		Remove:        os.Remove,
		RemoveAll:     os.RemoveAll,
		Stat:          os.Stat,
		Stdin:         os.Stdin,
		Stdout:        os.Stdout,
		TempFile:      ioutil.TempFile,
		Local:         time.Local,
	}
}
