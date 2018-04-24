package dbconn

/*
 * This file contains structs and functions related to connecting to a database
 * and executing queries.
 */

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // Need driver for postgres
	"github.com/pkg/errors"
)

/*
 * While the sqlx.DB struct (and indirectly the sql.DB struct) maintains its own
 * connection pool, there is no guarantee of session-level consistency between
 * queries and we require that level of control in some cases.  Also, while
 * sql.Conn is a struct that represents a single session, there is no
 * sqlx.Conn equivalent we could use.
 *
 * Thus, DBConn maintains its own connection pool of sqlx.DBs (all set to have
 * exactly one database connection each) in an array, such that callers can
 * create NumConns goroutines and assign each an index from 0 to NumConns to
 * guarantee that each goroutine gets its own connection that exhibits single-
 * session behavior.  The Exec, Select, and Get functions are set up to default
 * to the first connection (index 0), so the DBConn will still exhibit session-
 * like behavior if no connection is specified, and other functions that want to
 * execute in serial should pass in a 0 wherever a connection number is needed.
 */
type DBConn struct {
	ConnPool []*sqlx.DB
	NumConns int
	Driver   DBDriver
	User     string
	DBName   string
	Host     string
	Port     int
	Tx       []*sqlx.Tx
	Version  GPDBVersion
}

/*
 * Structs and functions for testing database functions
 */

type DBDriver interface {
	Connect(driverName string, dataSourceName string) (*sqlx.DB, error)
}

type GPDBDriver struct {
}

func (driver GPDBDriver) Connect(driverName string, dataSourceName string) (*sqlx.DB, error) {
	return sqlx.Connect(driverName, dataSourceName)
}

/*
 * Database functions
 */

func NewDBConnFromEnvironment(dbname string) *DBConn {
	if dbname == "" {
		gplog.Fatal(errors.New("No database provided"), "")
	}

	username := operating.System.Getenv("PGUSER")
	if username == "" {
		currentUser, _ := operating.System.CurrentUser()
		username = currentUser.Username
	}
	host := operating.System.Getenv("PGHOST")
	if host == "" {
		host, _ = operating.System.Hostname()
	}
	port, err := strconv.Atoi(operating.System.Getenv("PGPORT"))
	if err != nil {
		port = 5432
	}

	return NewDBConn(dbname, username, host, port)
}

func NewDBConn(dbname, username, host string, port int) *DBConn {
	if dbname == "" {
		gplog.Fatal(errors.New("No database provided"), "")
	}

	if username == "" {
		gplog.Fatal(errors.New("No username provided"), "")
	}

	if host == "" {
		gplog.Fatal(errors.New("No host provided"), "")
	}

	return &DBConn{
		ConnPool: nil,
		NumConns: 0,
		Driver:   GPDBDriver{},
		User:     username,
		DBName:   dbname,
		Host:     host,
		Port:     port,
		Tx:       nil,
		Version:  GPDBVersion{},
	}
}

func (dbconn *DBConn) MustBegin(whichConn ...int) {
	err := dbconn.Begin(whichConn...)
	gplog.FatalOnError(err)
}

func (dbconn *DBConn) Begin(whichConn ...int) error {
	connNum := dbconn.ValidateConnNum(whichConn...)
	if dbconn.Tx[connNum] != nil {
		return errors.New("Cannot begin transaction; there is already a transaction in progress")
	}
	var err error
	dbconn.Tx[connNum], err = dbconn.ConnPool[connNum].Beginx()
	if err != nil {
		return err
	}
	_, err = dbconn.Exec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", connNum)
	return err
}

func (dbconn *DBConn) Close() {
	if dbconn.ConnPool != nil {
		for _, conn := range dbconn.ConnPool {
			if conn != nil {
				conn.Close()
			}
		}
		dbconn.ConnPool = nil
		dbconn.Tx = nil
		dbconn.NumConns = 0
	}
}

func (dbconn *DBConn) MustCommit(whichConn ...int) {
	err := dbconn.Commit(whichConn...)
	gplog.FatalOnError(err)
}

func (dbconn *DBConn) Commit(whichConn ...int) error {
	connNum := dbconn.ValidateConnNum(whichConn...)
	if dbconn.Tx[connNum] == nil {
		return errors.New("Cannot commit transaction; there is no transaction in progress")
	}
	err := dbconn.Tx[connNum].Commit()
	dbconn.Tx[connNum] = nil
	return err
}

func (dbconn *DBConn) MustRollback(whichConn ...int) {
	err := dbconn.Rollback(whichConn...)
	gplog.FatalOnError(err)
}

func (dbconn *DBConn) Rollback(whichConn ...int) error {
	connNum := dbconn.ValidateConnNum(whichConn...)
	if dbconn.Tx[connNum] == nil {
		return errors.New("Cannot rollback transaction; there is no transaction in progress")
	}
	err := dbconn.Tx[connNum].Rollback()
	dbconn.Tx[connNum] = nil
	return err
}

func (dbconn *DBConn) MustConnect(numConns int) {
	err := dbconn.Connect(numConns)
	gplog.FatalOnError(err)
}

func (dbconn *DBConn) Connect(numConns int) error {
	if numConns < 1 {
		return errors.Errorf("Must specify a connection pool size that is a positive integer")
	}
	if dbconn.ConnPool != nil {
		return errors.Errorf("The database connection must be closed before reusing the connection")
	}
	dbname := EscapeConnectionParam(dbconn.DBName)
	user := EscapeConnectionParam(dbconn.User)
	connStr := fmt.Sprintf(`user='%s' dbname='%s' host=%s port=%d sslmode=disable`, user, dbname, dbconn.Host, dbconn.Port)
	dbconn.ConnPool = make([]*sqlx.DB, numConns)
	for i := 0; i < numConns; i++ {
		conn, err := dbconn.Driver.Connect("postgres", connStr)
		err = dbconn.handleConnectionError(err)
		if err != nil {
			return err
		}
		conn.SetMaxOpenConns(1)
		conn.SetMaxIdleConns(1)
		dbconn.ConnPool[i] = conn
	}
	dbconn.Tx = make([]*sqlx.Tx, numConns)
	dbconn.NumConns = numConns
	return nil
}

func (dbconn *DBConn) handleConnectionError(err error) error {
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			if strings.Contains(err.Error(), "pq: role") {
				return errors.Errorf(`Role "%s" does not exist, exiting`, dbconn.User)
			} else if strings.Contains(err.Error(), "pq: database") {
				return errors.Errorf(`Database "%s" does not exist, exiting`, dbconn.DBName)
			}
		} else if strings.Contains(err.Error(), "connection refused") {
			return errors.Errorf(`could not connect to server: Connection refused
	Is the server running on host "%s" and accepting
	TCP/IP connections on port %d?`, dbconn.Host, dbconn.Port)
		}
	}
	return err
}

/*
 * Wrapper functions for built-in sqlx and database/sql functionality; they will
 * automatically execute the query as part of an existing transaction if one is
 * in progress, to ensure that successive queries occur in one transaction without
 * requiring that to be ensured at the call site.
 */

func (dbconn *DBConn) Exec(query string, whichConn ...int) (sql.Result, error) {
	connNum := dbconn.ValidateConnNum(whichConn...)
	if dbconn.Tx[connNum] != nil {
		return dbconn.Tx[connNum].Exec(query)
	}
	return dbconn.ConnPool[connNum].Exec(query)
}

func (dbconn *DBConn) MustExec(query string, whichConn ...int) {
	_, err := dbconn.Exec(query, whichConn...)
	gplog.FatalOnError(err)
}

func (dbconn *DBConn) GetWithArgs(destination interface{}, query string, args ...interface{}) error {
	connNum := 0
	if dbconn.Tx[connNum] != nil {
		return dbconn.Tx[connNum].Get(destination, query, args...)
	}
	return dbconn.ConnPool[connNum].Get(destination, query, args...)
}

func (dbconn *DBConn) Get(destination interface{}, query string, whichConn ...int) error {
	connNum := dbconn.ValidateConnNum(whichConn...)
	if dbconn.Tx[connNum] != nil {
		return dbconn.Tx[connNum].Get(destination, query)
	}
	return dbconn.ConnPool[connNum].Get(destination, query)
}

func (dbconn *DBConn) SelectWithArgs(destination interface{}, query string, args ...interface{}) error {
	connNum := 0
	if dbconn.Tx[connNum] != nil {
		return dbconn.Tx[connNum].Select(destination, query, args...)
	}
	return dbconn.ConnPool[connNum].Select(destination, query, args...)
}

func (dbconn *DBConn) Select(destination interface{}, query string, whichConn ...int) error {
	connNum := dbconn.ValidateConnNum(whichConn...)
	if dbconn.Tx[connNum] != nil {
		return dbconn.Tx[connNum].Select(destination, query)
	}
	return dbconn.ConnPool[connNum].Select(destination, query)
}

/*
 * Ensure there isn't a mismatch between the connection pool size and number of
 * jobs, and default to using the first connection if no number is given.
 */
func (dbconn *DBConn) ValidateConnNum(whichConn ...int) int {
	if len(whichConn) == 0 {
		return 0
	}
	if len(whichConn) != 1 {
		gplog.Fatal(errors.Errorf("At most one connection number may be specified for a given connection"), "")
	}
	if whichConn[0] < 0 || whichConn[0] >= dbconn.NumConns {
		gplog.Fatal(errors.Errorf("Invalid connection number: %d", whichConn[0]), "")
	}
	return whichConn[0]
}

/*
 * Other useful/helper functions involving DBConn
 */

func EscapeConnectionParam(param string) string {
	param = strings.Replace(param, `\`, `\\`, -1)
	param = strings.Replace(param, `'`, `\'`, -1)
	return param
}

/*
 * This is a convenience function for Select() when we're selecting single string
 * that may be NULL or not exist.  We can't use Get() because that expects exactly
 * one string and will panic if no rows are returned, even if using a sql.NullString.
 */
func MustSelectString(connection *DBConn, query string, whichConn ...int) string {
	str, err := SelectString(connection, query, whichConn...)
	gplog.FatalOnError(err)
	return str
}

func SelectString(connection *DBConn, query string, whichConn ...int) (string, error) {
	results := make([]struct{ String string }, 0)
	connNum := connection.ValidateConnNum(whichConn...)
	err := connection.Select(&results, query, connNum)
	if err != nil {
		return "", err
	}
	if len(results) == 1 {
		return results[0].String, nil
	} else if len(results) > 1 {
		return "", errors.Errorf("Too many rows returned from query: got %d rows, expected 1 row", len(results))
	}
	return "", nil
}

// This is a convenience function for Select() when we're selecting single strings.
func MustSelectStringSlice(connection *DBConn, query string, whichConn ...int) []string {
	str, err := SelectStringSlice(connection, query, whichConn...)
	gplog.FatalOnError(err)
	return str
}

func SelectStringSlice(connection *DBConn, query string, whichConn ...int) ([]string, error) {
	results := make([]struct{ String string }, 0)
	connNum := connection.ValidateConnNum(whichConn...)
	err := connection.Select(&results, query, connNum)
	if err != nil {
		return []string{}, err
	}
	retval := make([]string, 0)
	for _, str := range results {
		if str.String != "" {
			retval = append(retval, str.String)
		}
	}
	return retval, nil
}
