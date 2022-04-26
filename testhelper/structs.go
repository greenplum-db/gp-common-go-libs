package testhelper

/*
 * This file contains test structs for dependency injection and functions on those structs.
 */

import (
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/jmoiron/sqlx"
)

type TestDriver struct {
	ErrToReturn  error
	ErrsToReturn []error
	DB           *sqlx.DB
	DBName       string
	User         string
	CallNumber   int
}

func (driver *TestDriver) Connect(driverName string, dataSourceName string) (*sqlx.DB, error) {
	if driver.ErrsToReturn != nil && driver.CallNumber < len(driver.ErrsToReturn) {
		// Return the errors in the order specified until we run out of specified errors, then return normally
		err := driver.ErrsToReturn[driver.CallNumber]
		driver.CallNumber++
		return nil, err
	} else if driver.ErrToReturn != nil {
		return nil, driver.ErrToReturn
	}
	return driver.DB, nil
}

type TestResult struct {
	Rows int64
}

func (result TestResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (result TestResult) RowsAffected() (int64, error) {
	return result.Rows, nil
}

type TestExecutor struct {
	LocalOutput     string
	LocalError      error
	LocalCommands   []string
	ClusterOutput   *cluster.RemoteOutput
	ClusterCommands [][]cluster.ShellCommand
	ErrorOnExecNum  int // Throw the specified error after this many executions of Execute[...]Command(); 0 means always return error
	NumExecutions   int
}

func (executor *TestExecutor) ExecuteLocalCommand(commandStr string) (string, error) {
	executor.NumExecutions++
	executor.LocalCommands = append(executor.LocalCommands, commandStr)
	if executor.ErrorOnExecNum == 0 || executor.NumExecutions == executor.ErrorOnExecNum {
		return executor.LocalOutput, executor.LocalError
	}
	return executor.LocalOutput, nil
}

func (executor *TestExecutor) ExecuteClusterCommand(scope cluster.Scope, commandList []cluster.ShellCommand) *cluster.RemoteOutput {
	executor.NumExecutions++
	executor.ClusterCommands = append(executor.ClusterCommands, commandList)
	if executor.ErrorOnExecNum == 0 || executor.NumExecutions == executor.ErrorOnExecNum {
		return executor.ClusterOutput
	}
	return nil
}
