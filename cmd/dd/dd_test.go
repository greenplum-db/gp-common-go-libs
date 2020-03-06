package main

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func testDD(t *testing.T, exePath string, exeFolder string) {
	outfilePath := filepath.Join(exeFolder, "outfile")
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
	for options, expected := range cases {
		out, err := exec.Command(exePath, options.Slice()...).CombinedOutput()
		if err != nil {
			t.Fatalf("%s %s: %v\n%s", exePath, options, err, string(out))
		}
		if options.OF != "" {
			out, err = ioutil.ReadFile(outfilePath)
			if err != nil {
				t.Fatalf("read output file error: %v", err)
			}
		}
		if string(out) != expected {
			t.Fatalf("output is %s, expected %s", string(out), expected)
		}
	}
}

func TestDD(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "dd")
	if err != nil {
		t.Fatal("TmpDir creation failed: ", err)
	}
	defer os.RemoveAll(tmpDir)

	ddPath := filepath.Join(tmpDir, GetCmd("dd"))
	goCmd := GetCmd("go")
	out, err := exec.Command(goCmd, "build", "-o", ddPath, "../dd").CombinedOutput()
	if err != nil {
		t.Fatalf("%s build -o %s ../dd: %s\n%s", goCmd, ddPath, err, string(out))
	}

	testDD(t, ddPath, tmpDir)
}

// Get command based on different OS
func GetCmd(cmd string) string {
	if runtime.GOOS == "windows" {
		cmd += ".exe"
	}
	return cmd
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
