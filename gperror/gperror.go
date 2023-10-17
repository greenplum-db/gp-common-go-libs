package gperror

import "fmt"

type ErrorCode uint32

type Error interface {
	error
	GetCode() ErrorCode
	GetErr() error
}

type GpError struct {
	Err error
	ErrorCode
}

func (e *GpError) Error() string {
	return fmt.Sprintf("ERROR[%04d] %s", e.GetCode(), e.Err.Error())
}

func (e *GpError) GetCode() ErrorCode {
	return e.ErrorCode
}

func (e *GpError) GetErr() error {
	return e.Err
}

func New(errorCode ErrorCode, errorFormat string, args ...any) Error {
	return &GpError{ErrorCode: errorCode, Err: fmt.Errorf(errorFormat, args...)}
}
