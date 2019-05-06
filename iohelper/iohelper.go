package iohelper

/*
 * This file contains generic file/directory manipulation functions that use
 * the function pointers and types from the operating package, for easier test
 * mocking around file IO.
 */

import (
	"bufio"
	"io"
	"os"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/pkg/errors"
)

/*
 * The following six OpenFileFor... and MustOpenFileFor... functions abstract
 * the most common cases for wanting to open files: when reading, open a read-
 * only handle and ignore file permissions as long as it's readable, and when
 * writing or appending, create the file if it doesn't exist with relatively
 * standard 644 permissions and then open it.
 *
 * If more niche or complex scenarios are involved, the underlying OpenFileRead
 * and OpenFileWrite functions should be used directly, to grant more fine-
 * grained control over flags and permissions.
 */

func OpenFileForReading(filename string) (operating.ReadCloserAt, error) {
	fileHandle, err := operating.System.OpenFileRead(filename, os.O_RDONLY, 0644)
	if err != nil {
		return nil, errors.Errorf("Unable to open file for reading: %s", err)
	}
	return fileHandle, nil
}

func MustOpenFileForReading(filename string) operating.ReadCloserAt {
	fileHandle, err := OpenFileForReading(filename)
	gplog.FatalOnError(err)
	return fileHandle
}

func OpenFileForWriting(filename string) (io.WriteCloser, error) {
	flags := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	fileHandle, err := operating.System.OpenFileWrite(filename, flags, 0644)
	if err != nil {
		return nil, errors.Errorf("Unable to create or open file for writing: %s", err)
	}
	return fileHandle, nil
}

func MustOpenFileForWriting(filename string) io.WriteCloser {
	fileHandle, err := OpenFileForWriting(filename)
	gplog.FatalOnError(err)
	return fileHandle
}

func OpenFileForAppending(filename string) (io.WriteCloser, error) {
	flags := os.O_APPEND | os.O_CREATE | os.O_WRONLY
	fileHandle, err := operating.System.OpenFileWrite(filename, flags, 0644)
	if err != nil {
		return nil, errors.Errorf("Unable to create or open file for appending: %s", err)
	}
	return fileHandle, nil
}

func MustOpenFileForAppending(filename string) io.WriteCloser {
	fileHandle, err := OpenFileForAppending(filename)
	gplog.FatalOnError(err)
	return fileHandle
}

func FileExistsAndIsReadable(filename string) bool {
	_, err := operating.System.Stat(filename)
	if err == nil {
		var fileHandle io.ReadCloser
		fileHandle, err = OpenFileForReading(filename)
		if fileHandle != nil {
			_ = fileHandle.Close()
		}
		if err == nil {
			return true
		}
	}
	return false
}

func ReadLinesFromFile(filename string) ([]string, error) {
	fileHandle, err := OpenFileForReading(filename)
	if err != nil {
		return nil, err
	}
	contents := make([]string, 0)
	scanner := bufio.NewScanner(fileHandle)
	for scanner.Scan() {
		contents = append(contents, scanner.Text())
	}
	err = fileHandle.Close()
	if err != nil {
		return nil, err
	}

	return contents, nil
}

func MustReadLinesFromFile(filename string) []string {
	contents, err := ReadLinesFromFile(filename)
	gplog.FatalOnError(err)
	return contents
}
