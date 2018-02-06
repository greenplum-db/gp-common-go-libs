package testhelper

/*
 * This file contains test structs for dependency injection and functions on those structs.
 */

import (
	"github.com/jmoiron/sqlx"
)

type TestDriver struct {
	ErrToReturn error
	DB          *sqlx.DB
	DBName      string
	User        string
}

func (driver TestDriver) Connect(driverName string, dataSourceName string) (*sqlx.DB, error) {
	if driver.ErrToReturn != nil {
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
