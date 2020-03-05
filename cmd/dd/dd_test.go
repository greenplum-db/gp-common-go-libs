// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"errors"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

const DDCmdName = "dd.exe"

func testDD(t *testing.T, execFolder string) {
	outfilePath := filepath.Join(execFolder, "outfile")
	cases := map[Options]string{
		Options{
			IF:    "dd.go",
			Count: 7,
		}: "package",
		Options{
			IF:    "dd.go",
			OF:    outfilePath,
			Skip:  8,
			Count: 4,
		}: "main",
	}
	exepath := filepath.Join(execFolder, DDCmdName)
	for options, expected := range cases {
		out, err := exec.Command(exepath, options.Slice()...).CombinedOutput()
		if err != nil {
			t.Fatalf("%s %s: %v\n%s", exepath, options, err, string(out))
		}
		if options.OF != "" {
			out, err = ioutil.ReadFile(outfilePath)
			if err != nil {
				t.Fatalf("read output file error: %v", err)
			}
		}
		if string(out) != expected {
			t.Fatalf("output is %s, expecte %s", string(out), expected)
		}
	}
}

func TestDD(t *testing.T) {
	MustHaveGoBuild(t)

	tmpDir, err := ioutil.TempDir("", "dd")
	if err != nil {
		t.Fatal("TempDir failed: ", err)
	}
	defer os.RemoveAll(tmpDir)

	goToolPath, err := GoTool()
	if err != nil {
		t.Fatal(err)
	}
	exepath := filepath.Join(tmpDir, DDCmdName)
	out, err := exec.Command(goToolPath, "build", "-o", exepath, "../dd").CombinedOutput()
	if err != nil {
		t.Fatalf("go build -o %v ./dd: %v\n%s", exepath, err, string(out))
	}

	testDD(t, tmpDir)
}

// Options dd command options struct
type Options struct {
	IF    string
	OF    string
	Skip  int
	Count int
}

// Slice returns options slice
func (opt Options) Slice() (slice []string) {
	if opt.IF != "" {
		slice = append(slice, "-if", opt.IF)
	}
	if opt.OF != "" {
		slice = append(slice, "-of", opt.OF)
	}
	if opt.Skip > 0 {
		slice = append(slice, "-skip", strconv.Itoa(opt.Skip))
	}
	if opt.Count > 0 {
		slice = append(slice, "-count", strconv.Itoa(opt.Count))
	}
	return
}

// String returns the options string
func (opt Options) String() (res string) {
	return strings.Join(opt.Slice(), " ")
}

// HasGoBuild reports whether the current system can build programs with ``go build''
// and then run them with os.StartProcess or exec.Command.
func HasGoBuild() bool {
	if os.Getenv("GO_GCFLAGS") != "" {
		// It's too much work to require every caller of the go command
		// to pass along "-gcflags="+os.Getenv("GO_GCFLAGS").
		// For now, if $GO_GCFLAGS is set, report that we simply can't
		// run go build.
		return false
	}
	switch runtime.GOOS {
	case "android", "nacl", "js":
		return false
	case "darwin":
		if strings.HasPrefix(runtime.GOARCH, "arm") {
			return false
		}
	}
	return true
}

// MustHaveGoBuild checks that the current system can build programs with ``go build''
// and then run them with os.StartProcess or exec.Command.
// If not, MustHaveGoBuild calls t.Skip with an explanation.
func MustHaveGoBuild(t testing.TB) {
	if os.Getenv("GO_GCFLAGS") != "" {
		t.Skipf("skipping test: 'go build' not compatible with setting $GO_GCFLAGS")
	}
	if !HasGoBuild() {
		t.Skipf("skipping test: 'go build' not available on %s/%s", runtime.GOOS, runtime.GOARCH)
	}
}

// GoTool reports the path to the Go tool.
func GoTool() (string, error) {
	if !HasGoBuild() {
		return "", errors.New("platform cannot run go tool")
	}
	var exeSuffix string
	if runtime.GOOS == "windows" {
		exeSuffix = ".exe"
	}
	path := filepath.Join(runtime.GOROOT(), "bin", "go"+exeSuffix)
	if _, err := os.Stat(path); err == nil {
		return path, nil
	}
	goBin, err := exec.LookPath("go" + exeSuffix)
	if err != nil {
		return "", errors.New("cannot find go tool: " + err.Error())
	}
	return goBin, nil
}
