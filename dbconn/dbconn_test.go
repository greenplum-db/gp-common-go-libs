package dbconn_test

import (
	"context"
	"database/sql/driver"
	"fmt"
	"os"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/jmoiron/sqlx"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var (
	connection *dbconn.DBConn
	mock       sqlmock.Sqlmock
)

func ExpectBegin(mock sqlmock.Sqlmock) {
	fakeResult := testhelper.TestResult{Rows: 0}
	mock.ExpectBegin()
	mock.ExpectExec("SET TRANSACTION(.*)").WillReturnResult(fakeResult)
}

func TestDBConn(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "dbconn tests")
}

var _ = BeforeSuite(func() {
	testhelper.SetupTestEnvironment()
})

var _ = BeforeEach(func() {
	connection, mock = testhelper.CreateAndConnectMockDB(1)
})

var _ = AfterEach(func() {
	if connection != nil {
		connection.Close()
	}
})

var _ = Describe("dbconn/dbconn tests", func() {
	BeforeEach(func() {
		operating.System.Now = func() time.Time { return time.Date(2017, time.January, 1, 1, 1, 1, 1, time.Local) }
	})
	Describe("NewDBConn", func() {
		It("gets the DBName from the dbname flag if it is set", func() {
			connection = dbconn.NewDBConnFromEnvironment("testdb")
			Expect(connection.DBName).To(Equal("testdb"))
		})
		It("gets the DB info", func() {
			connection = dbconn.NewDBConn("testdb", "testuser", "mars", 1234)
			Expect(connection.DBName).To(Equal("testdb"))
			Expect(connection.User).To(Equal("testuser"))
			Expect(connection.Host).To(Equal("mars"))
			Expect(connection.Port).To(Equal(1234))
		})
		It("fails if no database is given with the dbname flag", func() {
			defer testhelper.ShouldPanicWithMessage("No database provided")
			connection = dbconn.NewDBConnFromEnvironment("")
		})
		It("fails if username is an empty string", func() {
			defer testhelper.ShouldPanicWithMessage("No username provided")
			connection = dbconn.NewDBConn("testdb", "", "mars", 1234)
		})
		It("fails if host is an empty string", func() {
			defer testhelper.ShouldPanicWithMessage("No host provided")
			connection = dbconn.NewDBConn("testdb", "testuser", "", 1234)
		})
	})
	Describe("DBConn.MustConnect", func() {
		var mockdb *sqlx.DB
		BeforeEach(func() {
			connection, mock = testhelper.CreateMockDBConn()
			testhelper.ExpectVersionQuery(mock, "5.1.0")
		})
		It("makes a single connection successfully if the database exists", func() {
			connection.MustConnect(1)
			Expect(connection.DBName).To(Equal("testdb"))
			Expect(connection.NumConns).To(Equal(1))
			Expect(len(connection.ConnPool)).To(Equal(1))
			Expect(len(connection.Tx)).To(Equal(1))
		})
		It("makes multiple connections successfully if the database exists", func() {
			connection.MustConnect(3)
			Expect(connection.DBName).To(Equal("testdb"))
			Expect(connection.NumConns).To(Equal(3))
			Expect(len(connection.ConnPool)).To(Equal(3))
			Expect(len(connection.Tx)).To(Equal(3))
		})
		It("does not connect if the database exists but the connection is refused", func() {
			connection.Driver = &testhelper.TestDriver{ErrToReturn: fmt.Errorf("pq: connection refused"), DB: mockdb, User: "testrole"}
			defer testhelper.ShouldPanicWithMessage(`could not connect to server: Connection refused`)
			connection.MustConnect(1)
		})
		It("fails if an invalid number of connections is given", func() {
			defer testhelper.ShouldPanicWithMessage("Must specify a connection pool size that is a positive integer")
			connection.MustConnect(0)
		})
		It("fails if the database does not exist", func() {
			connection.Driver = &testhelper.TestDriver{ErrToReturn: fmt.Errorf("pq: database \"testdb\" does not exist"), DB: mockdb, DBName: "testdb", User: "testrole"}
			Expect(connection.DBName).To(Equal("testdb"))
			defer testhelper.ShouldPanicWithMessage("Database \"testdb\" does not exist on testhost:5432, exiting")
			connection.MustConnect(1)
		})
		It("fails if the role does not exist", func() {
			oldPgUser := os.Getenv("PGUSER")
			os.Setenv("PGUSER", "nonexistent")
			defer os.Setenv("PGUSER", oldPgUser)

			connection = dbconn.NewDBConnFromEnvironment("testdb")
			connection.Driver = &testhelper.TestDriver{ErrToReturn: fmt.Errorf("pq: role \"nonexistent\" does not exist"), DB: mockdb, DBName: "testdb", User: "nonexistent"}
			Expect(connection.User).To(Equal("nonexistent"))
			expectedStr := fmt.Sprintf("Role \"nonexistent\" does not exist on %s:%d, exiting", connection.Host, connection.Port)
			defer testhelper.ShouldPanicWithMessage(expectedStr)
			connection.MustConnect(1)
		})
	})
	Describe("DBConn.Connect", func() {
		It("can connect to GPDB 6 and earlier in utility mode", func() {
			connection, mock = testhelper.CreateMockDBConn(nil)
			testhelper.ExpectVersionQuery(mock, "6.0.0")

			err := connection.Connect(1, true)
			Expect(err).ToNot(HaveOccurred())
		})
		It("can connect to GPDB 7 and later in utility mode", func() {
			connection, mock = testhelper.CreateMockDBConn(fmt.Errorf(`pq: unrecognized configuration parameter "gp_session_role"`))
			testhelper.ExpectVersionQuery(mock, "7.0.0")

			err := connection.Connect(1, true)
			Expect(err).ToNot(HaveOccurred())
		})
		It("passes an error message on if a utility mode connection fails", func() {
			connection, mock = testhelper.CreateMockDBConn(fmt.Errorf(`pq: database \"testdb\" does not exist`))
			testhelper.ExpectVersionQuery(mock, "6.0.0")

			Expect(connection.DBName).To(Equal("testdb"))
			err := connection.Connect(1, true)
			Expect(err.Error()).To(Equal(`Database "testdb" does not exist on testhost:5432, exiting`))
		})
	})
	Describe("DBConn.Close", func() {
		BeforeEach(func() {
			connection, mock = testhelper.CreateMockDBConn()
			testhelper.ExpectVersionQuery(mock, "5.1.0")
		})
		It("successfully closes a dbconn with a single open connection", func() {
			connection.MustConnect(1)
			Expect(connection.NumConns).To(Equal(1))
			Expect(len(connection.ConnPool)).To(Equal(1))
			connection.Close()
			Expect(connection.NumConns).To(Equal(0))
			Expect(connection.ConnPool).To(BeNil())
		})
		It("successfully closes a dbconn with multiple open connections", func() {
			connection.MustConnect(3)
			Expect(connection.NumConns).To(Equal(3))
			Expect(len(connection.ConnPool)).To(Equal(3))
			Expect(len(connection.Tx)).To(Equal(3))
			connection.Close()
			Expect(connection.NumConns).To(Equal(0))
			Expect(connection.ConnPool).To(BeNil())
			Expect(connection.Tx).To(BeNil())
		})
		It("does nothing if there are no open connections", func() {
			connection.MustConnect(3)
			connection.Close()
			Expect(connection.NumConns).To(Equal(0))
			Expect(connection.ConnPool).To(BeNil())
			Expect(connection.Tx).To(BeNil())
			connection.Close()
			Expect(connection.NumConns).To(Equal(0))
			Expect(connection.ConnPool).To(BeNil())
			Expect(connection.Tx).To(BeNil())
		})
	})
	Describe("DBConn.Exec", func() {
		It("executes an INSERT outside of a transaction", func() {
			fakeResult := testhelper.TestResult{Rows: 1}
			mock.ExpectExec("INSERT (.*)").WillReturnResult(fakeResult)

			res, err := connection.Exec("INSERT INTO pg_tables VALUES ('schema', 'table')")
			Expect(err).ToNot(HaveOccurred())
			rowsReturned, err := res.RowsAffected()
			Expect(rowsReturned).To(Equal(int64(1)))
		})
		It("executes an INSERT in a transaction", func() {
			fakeResult := testhelper.TestResult{Rows: 1}
			ExpectBegin(mock)
			mock.ExpectExec("INSERT (.*)").WillReturnResult(fakeResult)
			mock.ExpectCommit()

			connection.MustBegin()
			res, err := connection.Exec("INSERT INTO pg_tables VALUES ('schema', 'table')")
			connection.MustCommit()
			Expect(err).ToNot(HaveOccurred())
			rowsReturned, err := res.RowsAffected()
			Expect(rowsReturned).To(Equal(int64(1)))
		})
	})
	Describe("DBConn.ExecContext", func() {
		It("executes an INSERT outside of a transaction", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			fakeResult := testhelper.TestResult{Rows: 1}
			mock.ExpectExec("INSERT (.*)").WillReturnResult(fakeResult)

			res, err := connection.ExecContext(ctx, "INSERT INTO pg_tables VALUES ('schema', 'table')")
			Expect(err).ToNot(HaveOccurred())
			rowsReturned, err := res.RowsAffected()
			Expect(rowsReturned).To(Equal(int64(1)))
		})
		It("executes an INSERT in a transaction", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			fakeResult := testhelper.TestResult{Rows: 1}
			ExpectBegin(mock)
			mock.ExpectExec("INSERT (.*)").WillReturnResult(fakeResult)
			mock.ExpectCommit()

			connection.MustBegin()
			res, err := connection.ExecContext(ctx, "INSERT INTO pg_tables VALUES ('schema', 'table')")
			connection.MustCommit()
			Expect(err).ToNot(HaveOccurred())
			rowsReturned, err := res.RowsAffected()
			Expect(rowsReturned).To(Equal(int64(1)))
		})
	})
	Describe("DBConn.Get", func() {
		It("executes a GET outside of a transaction", func() {
			two_col_single_row := sqlmock.NewRows([]string{"schemaname", "tablename"}).
				AddRow("schema1", "table1")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_col_single_row)

			testRecord := struct {
				Schemaname string
				Tablename  string
			}{}

			err := connection.Get(&testRecord, "SELECT schemaname, tablename FROM two_columns ORDER BY schemaname")

			Expect(err).ToNot(HaveOccurred())
			Expect(testRecord.Schemaname).To(Equal("schema1"))
			Expect(testRecord.Tablename).To(Equal("table1"))
		})
		It("executes a GET with argument outside of a transaction", func() {
			arg1 := "table1"
			arg2 := "table2"
			two_col_single_row := sqlmock.NewRows([]string{"schemaname", "tablename"}).
				AddRow("schema1", "table1")
			mock.ExpectQuery("SELECT (.*)").WithArgs(arg1, arg2).WillReturnRows(two_col_single_row)

			testRecord := struct {
				Schemaname string
				Tablename  string
			}{}

			err := connection.GetWithArgs(&testRecord, "SELECT schemaname, tablename FROM two_columns WHERE tablename=$1 OR tablename=$2", arg1, arg2)
			Expect(err).ToNot(HaveOccurred())
			Expect(testRecord.Schemaname).To(Equal("schema1"))
			Expect(testRecord.Tablename).To(Equal("table1"))
		})
		It("executes a GET in a transaction", func() {
			two_col_single_row := sqlmock.NewRows([]string{"schemaname", "tablename"}).
				AddRow("schema1", "table1")
			ExpectBegin(mock)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_col_single_row)
			mock.ExpectCommit()

			testRecord := struct {
				Schemaname string
				Tablename  string
			}{}

			connection.MustBegin()
			err := connection.Get(&testRecord, "SELECT schemaname, tablename FROM two_columns ORDER BY schemaname")
			connection.MustCommit()
			Expect(err).ToNot(HaveOccurred())
			Expect(testRecord.Schemaname).To(Equal("schema1"))
			Expect(testRecord.Tablename).To(Equal("table1"))
		})
	})
	Describe("DBConn.Select", func() {
		It("executes a SELECT outside of a transaction", func() {
			two_col_rows := sqlmock.NewRows([]string{"schemaname", "tablename"}).
				AddRow("schema1", "table1").
				AddRow("schema2", "table2")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_col_rows)

			testSlice := make([]struct {
				Schemaname string
				Tablename  string
			}, 0)

			err := connection.Select(&testSlice, "SELECT schemaname, tablename FROM two_columns ORDER BY schemaname LIMIT 2")

			Expect(err).ToNot(HaveOccurred())
			Expect(len(testSlice)).To(Equal(2))
			Expect(testSlice[0].Schemaname).To(Equal("schema1"))
			Expect(testSlice[0].Tablename).To(Equal("table1"))
			Expect(testSlice[1].Schemaname).To(Equal("schema2"))
			Expect(testSlice[1].Tablename).To(Equal("table2"))
		})
		It("executes a SELECT with argument outside of a transaction", func() {
			arg1 := "table1"
			arg2 := "table2"
			two_col_rows := sqlmock.NewRows([]string{"schemaname", "tablename"}).
				AddRow("schema1", "table1").
				AddRow("schema2", "table2")
			mock.ExpectQuery("SELECT (.*)").WithArgs(arg1, arg2).WillReturnRows(two_col_rows)

			testSlice := make([]struct {
				Schemaname string
				Tablename  string
			}, 0)

			err := connection.SelectWithArgs(&testSlice, "SELECT schemaname, tablename FROM two_columns WHERE tablename=$1 OR tablename=$2", arg1, arg2)

			Expect(err).ToNot(HaveOccurred())
			Expect(len(testSlice)).To(Equal(2))
			Expect(testSlice[0].Schemaname).To(Equal("schema1"))
			Expect(testSlice[0].Tablename).To(Equal("table1"))
			Expect(testSlice[1].Schemaname).To(Equal("schema2"))
			Expect(testSlice[1].Tablename).To(Equal("table2"))
		})
		It("executes a SELECT in a transaction", func() {
			two_col_rows := sqlmock.NewRows([]string{"schemaname", "tablename"}).
				AddRow("schema1", "table1").
				AddRow("schema2", "table2")
			ExpectBegin(mock)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_col_rows)
			mock.ExpectCommit()

			testSlice := make([]struct {
				Schemaname string
				Tablename  string
			}, 0)

			connection.MustBegin()
			err := connection.Select(&testSlice, "SELECT schemaname, tablename FROM two_columns ORDER BY schemaname LIMIT 2")
			connection.MustCommit()

			Expect(err).ToNot(HaveOccurred())
			Expect(len(testSlice)).To(Equal(2))
			Expect(testSlice[0].Schemaname).To(Equal("schema1"))
			Expect(testSlice[0].Tablename).To(Equal("table1"))
			Expect(testSlice[1].Schemaname).To(Equal("schema2"))
			Expect(testSlice[1].Tablename).To(Equal("table2"))
		})
	})
	Describe("DBConn.MustBegin", func() {
		It("successfully executes a BEGIN outside a transaction", func() {
			ExpectBegin(mock)
			connection.MustBegin()
			Expect(connection.Tx).To(Not(BeNil()))
		})
		It("panics if it executes a BEGIN in a transaction", func() {
			ExpectBegin(mock)
			connection.MustBegin()
			defer testhelper.ShouldPanicWithMessage("Cannot begin transaction; there is already a transaction in progress")
			connection.MustBegin()
		})
	})
	Describe("DBConn.MustCommit", func() {
		It("successfully executes a COMMIT in a transaction", func() {
			ExpectBegin(mock)
			mock.ExpectCommit()
			connection.MustBegin()
			connection.MustCommit()
			Expect(connection.Tx[0]).To(BeNil())
		})
		It("panics if it executes a COMMIT outside a transaction", func() {
			defer testhelper.ShouldPanicWithMessage("Cannot commit transaction; there is no transaction in progress")
			connection.MustCommit()
		})
	})
	Describe("Dbconn.ValidateConnNum", func() {
		BeforeEach(func() {
			connection, mock = testhelper.CreateMockDBConn()
			testhelper.ExpectVersionQuery(mock, "5.1.0")
			connection.MustConnect(3)
		})
		It("returns the connection number if it is valid", func() {
			num := connection.ValidateConnNum(1)
			Expect(num).To(Equal(1))
		})
		It("defaults to 0 with no argument", func() {
			num := connection.ValidateConnNum()
			Expect(num).To(Equal(0))
		})
		It("panics if given multiple arguments", func() {
			defer testhelper.ShouldPanicWithMessage("At most one connection number may be specified for a given connection")
			connection.ValidateConnNum(1, 2)
		})
		It("panics if given a negative number", func() {
			defer testhelper.ShouldPanicWithMessage("Invalid connection number: -1")
			connection.ValidateConnNum(-1)
		})
		It("panics if given a number greater than NumConns", func() {
			defer testhelper.ShouldPanicWithMessage("Invalid connection number: 4")
			connection.ValidateConnNum(4)
		})
	})
	Describe("MustSelectString", func() {
		header := []string{"foo"}
		rowOne := []driver.Value{"one"}
		rowTwo := []driver.Value{"two"}
		headerExtraCol := []string{"foo", "bar"}
		rowExtraCol := []driver.Value{"one", "two"}

		It("returns a single string if the query selects a single string", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			result := dbconn.MustSelectString(connection, "SELECT foo FROM bar")
			Expect(result).To(Equal("one"))
		})
		It("returns an empty string if the query selects no strings", func() {
			fakeResult := sqlmock.NewRows(header)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			result := dbconn.MustSelectString(connection, "SELECT foo FROM bar")
			Expect(result).To(Equal(""))
		})
		It("panics if the query selects multiple rows", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			defer testhelper.ShouldPanicWithMessage("Too many rows returned from query: got 2 rows, expected 1 row")
			dbconn.MustSelectString(connection, "SELECT foo FROM bar")
		})
		It("panics if the query selects multiple columns", func() {
			fakeResult := sqlmock.NewRows(headerExtraCol).AddRow(rowExtraCol...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			defer testhelper.ShouldPanicWithMessage("Too many columns returned from query: got 2 columns, expected 1 column")
			dbconn.MustSelectString(connection, "SELECT foo FROM bar")
		})
	})
	Describe("MustSelectStringSlice", func() {
		header := []string{"foo"}
		rowOne := []driver.Value{"one"}
		rowTwo := []driver.Value{"two"}
		headerExtraCol := []string{"foo", "bar"}
		rowExtraCol := []driver.Value{"one", "two"}

		It("returns a slice containing a single string if the query selects a single string", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := dbconn.MustSelectStringSlice(connection, "SELECT foo FROM bar")
			Expect(len(results)).To(Equal(1))
			Expect(results[0]).To(Equal("one"))
		})
		It("returns an empty slice if the query selects no strings", func() {
			fakeResult := sqlmock.NewRows(header)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := dbconn.MustSelectStringSlice(connection, "SELECT foo FROM bar")
			Expect(len(results)).To(Equal(0))
		})
		It("returns a slice containing multiple strings if the query selects multiple rows", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := dbconn.MustSelectStringSlice(connection, "SELECT foo FROM bar")
			Expect(len(results)).To(Equal(2))
			Expect(results[0]).To(Equal("one"))
			Expect(results[1]).To(Equal("two"))
		})
		It("panics if the query selects multiple columns", func() {
			fakeResult := sqlmock.NewRows(headerExtraCol).AddRow(rowExtraCol...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			defer testhelper.ShouldPanicWithMessage("Too many columns returned from query: got 2 columns, expected 1 column")
			dbconn.MustSelectString(connection, "SELECT foo FROM bar")
		})
	})
	Describe("MustSelectInt", func() {
		header := []string{"foo"}
		rowOne := []driver.Value{"1"}
		rowTwo := []driver.Value{"2"}
		headerExtraCol := []string{"foo", "bar"}
		rowExtraCol := []driver.Value{"1", "2"}

		It("returns a single int if the query selects a single int", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			result := dbconn.MustSelectInt(connection, "SELECT foo FROM bar")
			Expect(result).To(Equal(1))
		})
		It("returns 0 if the query selects no ints", func() {
			fakeResult := sqlmock.NewRows(header)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			result := dbconn.MustSelectInt(connection, "SELECT foo FROM bar")
			Expect(result).To(Equal(0))
		})
		It("panics if the query selects multiple rows", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			defer testhelper.ShouldPanicWithMessage("Too many rows returned from query: got 2 rows, expected 1 row")
			dbconn.MustSelectInt(connection, "SELECT foo FROM bar")
		})
		It("panics if the query selects multiple columns", func() {
			fakeResult := sqlmock.NewRows(headerExtraCol).AddRow(rowExtraCol...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			defer testhelper.ShouldPanicWithMessage("Too many columns returned from query: got 2 columns, expected 1 column")
			dbconn.MustSelectInt(connection, "SELECT foo FROM bar")
		})
	})
	Describe("MustSelectIntSlice", func() {
		header := []string{"foo"}
		rowOne := []driver.Value{"1"}
		rowTwo := []driver.Value{"2"}
		headerExtraCol := []string{"foo", "bar"}
		rowExtraCol := []driver.Value{"1", "2"}

		It("returns a slice containing a single int if the query selects a single int", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := dbconn.MustSelectIntSlice(connection, "SELECT foo FROM bar")
			Expect(len(results)).To(Equal(1))
			Expect(results[0]).To(Equal(1))
		})
		It("returns an empty slice if the query selects no ints", func() {
			fakeResult := sqlmock.NewRows(header)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := dbconn.MustSelectIntSlice(connection, "SELECT foo FROM bar")
			Expect(len(results)).To(Equal(0))
		})
		It("returns a slice containing multiple ints if the query selects multiple rows", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := dbconn.MustSelectIntSlice(connection, "SELECT foo FROM bar")
			Expect(len(results)).To(Equal(2))
			Expect(results[0]).To(Equal(1))
			Expect(results[1]).To(Equal(2))
		})
		It("panics if the query selects multiple columns", func() {
			fakeResult := sqlmock.NewRows(headerExtraCol).AddRow(rowExtraCol...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			defer testhelper.ShouldPanicWithMessage("Too many columns returned from query: got 2 columns, expected 1 column")
			dbconn.MustSelectInt(connection, "SELECT foo FROM bar")
		})
	})
})
