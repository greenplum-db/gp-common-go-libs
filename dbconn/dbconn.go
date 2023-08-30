package dbconn

/*
 * This file contains structs and functions related to connecting to a database
 * and executing queries.
 */

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"

	/*
	 * We previously used github.com/lib/pq as our Postgres driver,
	 * but it had a bug with the way it handled certain encodings.
	 * pgx seems to handle these encodings properly.
	 */
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
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

func (driver *GPDBDriver) Connect(driverName string, dataSourceName string) (*sqlx.DB, error) {
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
		Driver:   &GPDBDriver{},
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
				_ = conn.Close()
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

func (dbconn *DBConn) Connect(numConns int, utilityMode ...bool) error {
	if numConns < 1 {
		return errors.Errorf("Must specify a connection pool size that is a positive integer")
	}
	if dbconn.ConnPool != nil {
		return errors.Errorf("The database connection must be closed before reusing the connection")
	}

	dbname := EscapeConnectionParam(dbconn.DBName)
	user := EscapeConnectionParam(dbconn.User)
	krbsrvname := operating.System.Getenv("PGKRBSRVNAME")
	if krbsrvname == "" {
		krbsrvname = "postgres"
	}
	sslmode := operating.System.Getenv("PGSSLMODE")
	if sslmode == "" {
		sslmode = "prefer"
	}
	// This string takes in the literal user/database names. They do not need
	// to be escaped or quoted.
	// By default pgx/v4 turns on automatic prepared statement caching. This
	// causes an issue in GPDB4 where creating an object, deleting it, creating
	// the same object again, then querying for the object in the same
	// connection will generate a cache lookup failure. To disable pgx's
	// automatic prepared statement cache we set statement_cache_capacity to 0.
	connStr := fmt.Sprintf(`user='%s' dbname='%s' krbsrvname='%s' host=%s port=%d sslmode='%s' statement_cache_capacity=0`,
		user, dbname, krbsrvname, dbconn.Host, dbconn.Port, sslmode)

	dbconn.ConnPool = make([]*sqlx.DB, numConns)
	if len(utilityMode) > 1 {
		return errors.Errorf("The utility mode parameter accepts exactly one boolean value")
	} else if len(utilityMode) == 1 && utilityMode[0] {
		// The utility mode GUC differs between GPDB 7 and later (gp_role)
		// and GPDB 6 and earlier (gp_session_role), and we don't get the
		// database version until after the connection is established, so
		// we need to just try one first and see whether it works.
		roleConnStr := connStr + " gp_role=utility"
		sessionRoleConnStr := connStr + " gp_session_role=utility"
		utilConn, err := dbconn.Driver.Connect("pgx", sessionRoleConnStr)
		if utilConn != nil {
			utilConn.Close()
		}
		if err != nil {
			if strings.Contains(err.Error(), `unrecognized configuration parameter "gp_session_role"`) {
				connStr = roleConnStr
			} else {
				return dbconn.handleConnectionError(err)
			}
		} else {
			connStr = sessionRoleConnStr
		}
	}

	for i := 0; i < numConns; i++ {
		conn, err := dbconn.Driver.Connect("pgx", connStr)
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
	version, err := InitializeVersion(dbconn)
	if err != nil {
		return errors.Wrap(err, "Failed to determine database version")
	}
	dbconn.Version = version
	return nil
}

func (dbconn *DBConn) MustConnectInUtilityMode(numConns int) {
	err := dbconn.Connect(numConns, true)
	gplog.FatalOnError(err)
}

func (dbconn *DBConn) ConnectInUtilityMode(numConns int) error {
	return dbconn.Connect(numConns, true)
}

func (dbconn *DBConn) handleConnectionError(err error) error {
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			if strings.Contains(err.Error(), "pq: role") {
				return errors.Errorf(`Role "%s" does not exist on %s:%d, exiting`, dbconn.User, dbconn.Host, dbconn.Port)
			} else if strings.Contains(err.Error(), "pq: database") {
				return errors.Errorf(`Database "%s" does not exist on %s:%d, exiting`, dbconn.DBName, dbconn.Host, dbconn.Port)
			}
		} else if strings.Contains(err.Error(), "connection refused") {
			return errors.Errorf(`could not connect to server: Connection refused
	Is the server running on host "%s" and accepting
	TCP/IP connections on port %d?`, dbconn.Host, dbconn.Port)
		} else {
			return errors.Errorf("%v (%s:%d)", err, dbconn.Host, dbconn.Port)
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

func (dbconn *DBConn) ExecContext(queryContext context.Context, query string, whichConn ...int) (sql.Result, error) {
	connNum := dbconn.ValidateConnNum(whichConn...)
	if dbconn.Tx[connNum] != nil {
		return dbconn.Tx[connNum].ExecContext(queryContext, query)
	}
	return dbconn.ConnPool[connNum].ExecContext(queryContext, query)
}

func (dbconn *DBConn) MustExecContext(queryContext context.Context, query string, whichConn ...int) {
	_, err := dbconn.ExecContext(queryContext, query, whichConn...)
	gplog.FatalOnError(err)
}

func (dbconn *DBConn) GetWithArgs(destination interface{}, query string, args ...interface{}) error {
	if dbconn.Tx[0] != nil {
		return dbconn.Tx[0].Get(destination, query, args...)
	}
	return dbconn.ConnPool[0].Get(destination, query, args...)
}

func (dbconn *DBConn) Get(destination interface{}, query string, whichConn ...int) error {
	connNum := dbconn.ValidateConnNum(whichConn...)
	if dbconn.Tx[connNum] != nil {
		return dbconn.Tx[connNum].Get(destination, query)
	}
	return dbconn.ConnPool[connNum].Get(destination, query)
}

func (dbconn *DBConn) SelectWithArgs(destination interface{}, query string, args ...interface{}) error {
	if dbconn.Tx[0] != nil {
		return dbconn.Tx[0].Select(destination, query, args...)
	}
	return dbconn.ConnPool[0].Select(destination, query, args...)
}

func (dbconn *DBConn) Select(destination interface{}, query string, whichConn ...int) error {
	connNum := dbconn.ValidateConnNum(whichConn...)
	if dbconn.Tx[connNum] != nil {
		return dbconn.Tx[connNum].Select(destination, query)
	}
	return dbconn.ConnPool[connNum].Select(destination, query)
}

func (dbconn *DBConn) QueryWithArgs(query string, args ...interface{}) (*sqlx.Rows, error) {
	if dbconn.Tx[0] != nil {
		return dbconn.Tx[0].Queryx(query, args...)
	}
	return dbconn.ConnPool[0].Queryx(query, args...)
}

func (dbconn *DBConn) Query(query string, whichConn ...int) (*sqlx.Rows, error) {
	connNum := dbconn.ValidateConnNum(whichConn...)
	if dbconn.Tx[connNum] != nil {
		return dbconn.Tx[connNum].Queryx(query)
	}
	return dbconn.ConnPool[connNum].Queryx(query)
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
 * This is a convenience function for Select() when we're selecting a single
 * string that may be NULL or not exist.  We can't use Get() because that
 * expects exactly one string and will panic if no rows are returned, even if
 * using a sql.NullString.
 *
 * SelectString calls SelectStringSlice and returns the first value instead of
 * calling QueryRowx because that function doesn't indicate if there were more
 * rows available to be returned, and we don't want to silently ignore that if
 * only one row was expected for a given query but multiple were returned.
 */
func MustSelectString(connection *DBConn, query string, whichConn ...int) string {
	str, err := SelectString(connection, query, whichConn...)
	gplog.FatalOnError(err)
	return str
}

func SelectString(connection *DBConn, query string, whichConn ...int) (string, error) {
	results, err := SelectStringSlice(connection, query, whichConn...)
	if err != nil {
		return "", err
	}
	if len(results) == 1 {
		return results[0], nil
	} else if len(results) > 1 {
		return "", errors.Errorf("Too many rows returned from query: got %d rows, expected 1 row", len(results))
	}
	return "", nil
}

/*
 * This is a convenience function for Select() when we're selecting a single
 * column of strings that may be NULL.  Select requires defining a struct for
 * each call, and this function uses the underlying sql functions instead of
 * sqlx functions to avoid needing to "SELECT [column] AS [struct field]" with
 * a generic struct or the like.
 *
 * It also gives a nicer error message in the event that a query is called with
 * multiple columns, where using a generic struct gives an opaque "missing
 * destination name" error.
 */
func MustSelectStringSlice(connection *DBConn, query string, whichConn ...int) []string {
	str, err := SelectStringSlice(connection, query, whichConn...)
	gplog.FatalOnError(err)
	return str
}

func SelectStringSlice(connection *DBConn, query string, whichConn ...int) ([]string, error) {
	connNum := connection.ValidateConnNum(whichConn...)
	rows, err := connection.Query(query, connNum)
	if err != nil {
		return []string{}, err
	}
	if cols, _ := rows.Rows.Columns(); len(cols) > 1 {
		return []string{}, errors.Errorf("Too many columns returned from query: got %d columns, expected 1 column", len(cols))
	}
	retval := make([]string, 0)
	for rows.Rows.Next() {
		var result sql.NullString
		err = rows.Rows.Scan(&result)
		if err != nil {
			return []string{}, err
		}
		retval = append(retval, result.String)
	}
	if rows.Rows.Err() != nil {
		return []string{}, rows.Rows.Err()
	}
	return retval, nil
}

/*
 * The below are convenience functions for selecting one or more ints that may
 * be NULL or may not exist, with the same functionality (and the same rationale)
 * as SelectString and SelectStringSlice; see the comments for those functions,
 * above, for more details.
 */
func MustSelectInt(connection *DBConn, query string, whichConn ...int) int {
	str, err := SelectInt(connection, query, whichConn...)
	gplog.FatalOnError(err)
	return str
}

func SelectInt(connection *DBConn, query string, whichConn ...int) (int, error) {
	results, err := SelectIntSlice(connection, query, whichConn...)
	if err != nil {
		return 0, err
	}
	if len(results) == 1 {
		return results[0], nil
	} else if len(results) > 1 {
		return 0, errors.Errorf("Too many rows returned from query: got %d rows, expected 1 row", len(results))
	}
	return 0, nil
}

func MustSelectIntSlice(connection *DBConn, query string, whichConn ...int) []int {
	str, err := SelectIntSlice(connection, query, whichConn...)
	gplog.FatalOnError(err)
	return str
}

func SelectIntSlice(connection *DBConn, query string, whichConn ...int) ([]int, error) {
	connNum := connection.ValidateConnNum(whichConn...)
	rows, err := connection.Query(query, connNum)
	if err != nil {
		return []int{}, err
	}
	if cols, _ := rows.Rows.Columns(); len(cols) > 1 {
		return []int{}, errors.Errorf("Too many columns returned from query: got %d columns, expected 1 column", len(cols))
	}
	retval := make([]int, 0)
	for rows.Rows.Next() {
		var result sql.NullInt32
		err = rows.Rows.Scan(&result)
		if err != nil {
			return []int{}, err
		}
		retval = append(retval, int(result.Int32))
	}
	if rows.Rows.Err() != nil {
		return []int{}, rows.Rows.Err()
	}
	return retval, nil
}
