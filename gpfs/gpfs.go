package gpfs

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"syscall"

	vfs "github.com/spf13/afero"
)

type FsObjectType int

const (
	IsFile FsObjectType = iota
	IsDir
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

//counterfeiter:generate . GpFs
type GpFs interface {
	Append(filename string, content string, perm os.FileMode) (int, error)
	CheckExists(fullPath string, fsObjectType FsObjectType) (bool, error)
	Chmod(name string, perm os.FileMode) error
	Chown(name string, user string, group string) error
	Copy(source string, dest string, perm fs.FileMode) error
	CreateDir(fullPath string) error
	Delete(fullPath string) error
	GetFileMode(name string) (os.FileMode, error)
	GetUidAndGidForPath(path string) (int, int, error)
	Glob(fullPath string) ([]string, error)
	MatchPattern(fullpath string, pattern string) ([]string, error)
	MatchString(fullpath string, str string) ([]string, error)
	Read(fullPath string) (string, error)
	RecursiveChown(path string, user string, group string) error
	Write(filename string, content string, perm os.FileMode) error
}

var Dependency struct {
	Chmod                func(fs vfs.Fs, name string, perms os.FileMode) error
	Chown                func(fs vfs.Fs, fullPath string, user string, group string) error
	Copy                 func(dest io.Writer, source io.Reader) (int64, error)
	CreateDir            func(fs vfs.Fs, fullPath string) error
	GetChownWalkFunc     func(fs vfs.Fs, user string, group string) filepath.WalkFunc
	GetGid               func(groupname string) (int, error)
	GetMatches           func(f GpFile, r GpRegexp) []string
	GetScanner           func(f GpFile) GpScanner
	GetUid               func(username string) (int, error)
	GetUidAndGidFromFile func(fileInfo OsFile) (int, int)
	Glob                 func(fs vfs.Fs, pattern string) ([]string, error)
	LookupGroup          func(groupname string) (*user.Group, error)
	LookupUser           func(username string) (*user.User, error)
	MatchLines           func(fs vfs.Fs, fullPath string, pattern string) ([]string, error)
	OpenDestFile         func(fs vfs.Fs, path string, flags int, perms os.FileMode) (GpFile, error)
	OpenFile             func(fs vfs.Fs, path string, flags int, perms os.FileMode) (GpFile, error)
	OpenSourceFile       func(fs vfs.Fs, path string, flags int, perms os.FileMode) (GpFile, error)
	ReadFile             func(fs vfs.Fs, fullPath string) ([]byte, error)
	RegexpCompile        func(pattern string) (GpRegexp, error)
	Stat                 func(fs vfs.Fs, path string) (OsFile, error)
	Walk                 func(fs vfs.Fs, root string, walkFn filepath.WalkFunc) error
	WriteFile            func(fs vfs.Fs, filename string, data []byte, perm os.FileMode) error
}

func init() {
	Default()
}

func Default() {
	Dependency.Chmod = Chmod
	Dependency.Chown = Chown
	Dependency.Copy = io.Copy
	Dependency.CreateDir = CreateDir
	Dependency.GetChownWalkFunc = GetChownWalkFunc
	Dependency.GetGid = GetGid
	Dependency.GetMatches = GetMatches
	Dependency.GetScanner = getScanner
	Dependency.GetUid = GetUid
	Dependency.GetUidAndGidFromFile = getUidAndGidFromFile
	Dependency.Glob = glob
	Dependency.LookupGroup = user.LookupGroup
	Dependency.LookupUser = user.Lookup
	Dependency.MatchLines = MatchLines
	Dependency.OpenDestFile = OpenFile
	Dependency.OpenFile = OpenFile
	Dependency.OpenSourceFile = OpenFile
	Dependency.ReadFile = readFile
	Dependency.RegexpCompile = RegexpCompile
	Dependency.Stat = Stat
	Dependency.Walk = walk
	Dependency.WriteFile = writeFile
}

//counterfeiter:generate . Filesystem
type Filesystem vfs.Fs

//counterfeiter:generate . GpFile
type GpFile vfs.File

//counterfeiter:generate . OsFile
type OsFile os.FileInfo

type GpFsImpl struct {
	vfs.Fs
}

func New() GpFs {
	return &GpFsImpl{Fs: vfs.NewOsFs()}
}

func NewMockFs() GpFs {
	return &GpFsImpl{Fs: vfs.NewMemMapFs()}
}

func (fs GpFsImpl) Append(fullPath string, content string, perms os.FileMode) (int, error) {
	if err := Dependency.CreateDir(fs.Fs, path.Dir(fullPath)); err != nil {
		return 0, err
	}

	f, err := Dependency.OpenFile(fs.Fs, fullPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, perms)
	if err != nil {
		return 0, fmt.Errorf(`writing "%s" failed due to: %w`, fullPath, err)
	}
	defer f.Close()

	count, err := f.WriteString(content)
	if err != nil {
		return 0, fmt.Errorf(`writing "%s" failed due to: %w`, fullPath, err)
	}

	return count, nil
}

func (fs GpFsImpl) CheckExists(fullPath string, fsObjectType FsObjectType) (bool, error) {
	file, err := fs.Stat(fullPath)
	if errors.Is(err, os.ErrNotExist) {
		// File doesn't exist. This is an expected error.
		return false, nil
	} else if err != nil {
		// This is an unexpected error.
		return false, fmt.Errorf(`getting system info for "%s" failed due to: %w`, fullPath, err)
	}

	// Assume that file must be non-nil if err is nil
	switch fsObjectType {
	case IsDir:
		if !file.IsDir() {
			return false, fmt.Errorf(`expected directory "%s" but found a non-directory instead`, fullPath)
		}
	case IsFile:
		if file.IsDir() {
			return false, fmt.Errorf(`expected non-directory "%s" but found a directory instead`, fullPath)
		}
	}

	return true, nil
}

func (fs GpFsImpl) Chmod(name string, perms os.FileMode) error {
	return Dependency.Chmod(fs.Fs, name, perms)
}

func (fs GpFsImpl) Chown(fullPath string, user string, group string) error {
	return Dependency.Chown(fs.Fs, fullPath, user, group)
}

func (fs GpFsImpl) Copy(source string, dest string, perms fs.FileMode) error {
	s, err := Dependency.OpenSourceFile(fs.Fs, source, os.O_RDONLY, perms)
	if err != nil {
		return err
	}
	defer s.Close()

	d, err := Dependency.OpenDestFile(fs.Fs, dest, os.O_WRONLY|os.O_CREATE, perms)
	if err != nil {
		return err
	}
	defer d.Close()

	if _, err := Dependency.Copy(d, s); err != nil {
		return err
	}

	return nil
}

func (fs GpFsImpl) CreateDir(fullPath string) error {
	return Dependency.CreateDir(fs.Fs, fullPath)
}

func (fs GpFsImpl) Delete(fullPath string) error {
	if err := fs.Fs.RemoveAll(fullPath); err != nil {
		return fmt.Errorf(`removing "%s" failed due to: %w`, fullPath, err)
	}

	return nil
}

func (fs GpFsImpl) GetFileMode(fullPath string) (os.FileMode, error) {
	fileInfo, err := Dependency.Stat(fs.Fs, fullPath)
	if err != nil {
		return 0, err
	}

	return fileInfo.Mode().Perm(), nil
}

func (fs GpFsImpl) GetUidAndGidForPath(path string) (int, int, error) {
	fileInfo, err := Dependency.Stat(fs.Fs, path)
	if err != nil {
		return 0, 0, err
	}

	uid, gid := Dependency.GetUidAndGidFromFile(fileInfo)
	return uid, gid, nil
}

func (fs GpFsImpl) Glob(pattern string) ([]string, error) {
	if matches, err := Dependency.Glob(fs.Fs, pattern); err != nil {
		return nil, fmt.Errorf(`failed to match "%s" due to: %w`, pattern, err)
	} else {
		return matches, nil
	}
}

func (fs GpFsImpl) MatchPattern(fullPath string, pattern string) ([]string, error) {
	return Dependency.MatchLines(fs.Fs, fullPath, pattern)
}

func (fs GpFsImpl) MatchString(fullPath string, str string) ([]string, error) {
	return Dependency.MatchLines(fs.Fs, fullPath, fmt.Sprintf("^%s$", str))
}

func (fs GpFsImpl) Read(fullPath string) (string, error) {
	if content, err := Dependency.ReadFile(fs.Fs, fullPath); err != nil {
		return "", fmt.Errorf(`failed to read "%s" due to: %w`, fullPath, err)
	} else {
		return string(content), nil
	}
}

func (fs GpFsImpl) RecursiveChown(path string, user string, group string) error {
	// Rely on Chown in GetChownWalkFunc() to format the error
	return Dependency.Walk(fs.Fs, path, Dependency.GetChownWalkFunc(fs.Fs, user, group))
}

func (fs GpFsImpl) Write(filename string, content string, perms os.FileMode) error {
	if err := Dependency.CreateDir(fs.Fs, path.Dir(filename)); err != nil {
		return err
	}

	if err := Dependency.WriteFile(fs.Fs, filename, []byte(content), perms); err != nil {
		return fmt.Errorf(`failed to write "%s" due to: %w`, filename, err)
	}

	if err := Dependency.Chmod(fs.Fs, filename, perms); err != nil {
		return err
	}

	return nil
}

func Chmod(fs vfs.Fs, name string, perms os.FileMode) error {
	if err := fs.Chmod(name, perms); err != nil {
		return fmt.Errorf(`changing permissions for "%s" failed due to: %w`, name, err)
	}

	return nil
}

func Chown(fs vfs.Fs, fullPath string, user string, group string) error {
	uid, err := Dependency.GetUid(user)
	if err != nil {
		return err
	}

	gid, err := Dependency.GetGid(group)
	if err != nil {
		return err
	}

	if err := fs.Chown(fullPath, uid, gid); err != nil {
		return fmt.Errorf(`changing ownership of "%s" to "%s:%s" failed due to: %w`, fullPath, user, group, err)
	}

	return nil
}

func CreateDir(fs vfs.Fs, fullPath string) error {
	if err := fs.MkdirAll(fullPath, 0755); err != nil {
		return fmt.Errorf(`creating "%s" failed due to: %w`, fullPath, err)
	}

	return nil
}

func GetChownWalkFunc(fs vfs.Fs, user string, group string) filepath.WalkFunc {
	return func(name string, _ os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		return Dependency.Chown(fs, name, user, group)
	}
}

func GetGid(groupname string) (int, error) {
	group, err := Dependency.LookupGroup(groupname)
	if err != nil {
		return -1, fmt.Errorf(`fetching group ID for "%s" failed due to: %w`, groupname, err)
	}

	gid, err := strconv.Atoi(group.Gid)
	if err != nil {
		return -1, fmt.Errorf(`unable to convert group ID "%s" into an integer`, group.Gid)
	}

	return gid, nil
}

func GetHomeDir(username string) (string, error) {
	userInfo, err := Dependency.LookupUser(username)
	if err != nil {
		return "", err
	}

	return userInfo.HomeDir, nil
}

func GetMatches(f GpFile, r GpRegexp) []string {
	scanner := Dependency.GetScanner(f)

	matches := []string{}
	for scanner.Scan() {
		line := scanner.Text()
		if r.MatchString(line) {
			matches = append(matches, line)
		}
	}

	return matches
}

func GetUid(username string) (int, error) {
	userInfo, err := Dependency.LookupUser(username)
	if err != nil {
		return -1, fmt.Errorf(`unable to lookup user "%s" failed due to: %w`, username, err)
	}

	uid, err := strconv.Atoi(userInfo.Uid)
	if err != nil {
		return -1, fmt.Errorf(`unable to convert user ID "%s" into an integer`, userInfo.Uid)
	}

	return uid, nil
}

func MatchLines(fs vfs.Fs, fullPath string, pattern string) ([]string, error) {
	r, err := Dependency.RegexpCompile(pattern)
	if err != nil {
		return nil, err
	}
	f, err := Dependency.OpenFile(fs, fullPath, os.O_RDONLY, 0600)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return Dependency.GetMatches(f, r), nil
}

func OpenFile(fs vfs.Fs, path string, flags int, perms os.FileMode) (GpFile, error) {
	if f, err := fs.OpenFile(path, flags, perms); err != nil {
		return nil, fmt.Errorf(`opening "%s" failed due to: %w`, path, err)
	} else {
		return f, nil
	}
}

//counterfeiter:generate . GpRegexp
type GpRegexp interface {
	MatchString(text string) bool
}

func RegexpCompile(pattern string) (GpRegexp, error) {
	r, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func Stat(fs vfs.Fs, path string) (OsFile, error) {
	fileInfo, err := fs.Stat(path)
	if err != nil {
		return nil, fmt.Errorf(`failed to fetch file permissions for "%s" due to: %w`, path, err)
	}

	return fileInfo, nil
}

//counterfeiter:generate . GpScanner
type GpScanner interface {
	Scan() bool
	Text() string
}

func getScanner(f GpFile) GpScanner {
	return bufio.NewScanner(f)
}

func getUidAndGidFromFile(fileInfo OsFile) (int, int) {
	stat := fileInfo.Sys().(*syscall.Stat_t)

	return int(stat.Uid), int(stat.Gid)
}

func glob(fs vfs.Fs, pattern string) ([]string, error) {
	return vfs.Glob(fs, pattern)
}

func readFile(fs vfs.Fs, fullPath string) ([]byte, error) {
	return vfs.ReadFile(fs, fullPath)
}

func walk(fs vfs.Fs, path string, walkFn filepath.WalkFunc) error {
	return vfs.Walk(fs, path, walkFn)
}

func writeFile(fs vfs.Fs, filename string, content []byte, perm os.FileMode) error {
	return vfs.WriteFile(fs, filename, content, perm)
}
