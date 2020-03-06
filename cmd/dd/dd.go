package main

import (
	"flag"
	"fmt"
	"io"
	"os"
)

const usageMessage = "" +
	`Usage:
dd -if [input FILE] -of [output FILE] -skip [skip bytes] -count [bytes to copy]

Examples:
Copy 100 bytes from a.in to b.out, skipping the starting 20 bytes:
	dd -if a.in -of b.out -skip 20 -count 100

Print the beginning 100 bytes from a.in to stdout:
	dd -if a.in -count 100
`

var (
	// main operation modes
	ifPath = flag.String("if", "", "read from FILE")
	skip   = flag.Int64("skip", 0, "skip N bytes at start of input FILE, default is 0")
	count  = flag.Int64("count", 0, "copy only N bytes, default is 0")
	ofPath = flag.String("of", "", "write to FILE, default is stdout")
)

var exitCode = 0

func usage() {
	fmt.Fprintln(os.Stderr, usageMessage)
	fmt.Fprintln(os.Stderr, "Flags:")
	flag.PrintDefaults()
}

func validate() (isSuccess bool, field string, message string) {
	// process skip flag
	if skip == nil || *skip < 0 {
		return false, "skip", "must >= 0"
	}

	// process count flag
	if count == nil || *count <= 0 {
		return false, "count", "must > 0"
	}

	// process if flag, and seek in inFile to set offset
	if ifPath == nil || *ifPath == "" {
		return false, "if", "No input file provided"
	}

	return true, "", ""
}

func prepareInFile(offset int64) (*os.File, error) {
	inFile, err := os.Open(*ifPath)
	if err != nil {
		return nil, err
	}

	_, err = inFile.Seek(offset, 0)
	if err != nil {
		inFile.Close()
		return nil, err
	}
	return inFile, nil
}

func prepareOutFile() (outFile *os.File, err error) {
	return os.OpenFile(*ofPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
}

func report(err error) {
	fmt.Fprintln(os.Stderr, err)
	exitCode = 1
}

func main() {
	ddMain()
	os.Exit(exitCode)
}

func ddMain() {
	flag.Usage = usage
	flag.Parse()

	var inFile, outFile *os.File

	// do simple validations, fail fast
	if isSuccess, field, msg := validate(); !isSuccess {
		report(fmt.Errorf("%s: %s", field, msg))
		flag.Usage()
		return
	}

	// prepare inFile, and seek in inFile to set offset
	inFile, err := prepareInFile(*skip)
	if err != nil {
		report(err)
		return
	}
	defer inFile.Close()

	// prepare outFile
	if ofPath == nil || *ofPath == "" {
		// output default to stdout when "of" not provided
		outFile = os.Stdout
	}
	if outFile == nil {
		outFile, err = prepareOutFile()
		if err != nil {
			report(err)
			return
		}
		defer outFile.Close()
	}

	// copy [count] bytes from inFile to outFile
	_, err = io.CopyN(outFile, inFile, *count)
	if err != nil {
		report(err)
	}
}
