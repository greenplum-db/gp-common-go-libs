package activerecord

import "database/sql"

// MockableActiveRecord is a mock AR for test usage
type MockableActiveRecord interface {
	GetRows() ([]map[string]interface{}, error)
	GetRow() (map[string]interface{}, error)
	GetRowsI(query string, args ...interface{}) (result []map[string]interface{}, err error)
	Exec() (sql.Result, error)
	GetCount(args ...interface{}) (count int, err error)
	Connect(dbtype string, url string) (err error)
	ExecSQL(sql string, args ...interface{}) (sql.Result, error)
	Select(fields ...string) MockableActiveRecord
	SelectDistinct(fields ...string) MockableActiveRecord
	From(tables ...string) MockableActiveRecord
	Join(table string) MockableActiveRecord
	InnerJoin(table string) MockableActiveRecord
	LeftJoin(table string) MockableActiveRecord
	RightJoin(table string) MockableActiveRecord
	LeftOuterJoin(table string) MockableActiveRecord
	RightOuterJoin(table string) MockableActiveRecord
	On(cond string, args ...interface{}) MockableActiveRecord
	Where(cond string, args ...interface{}) MockableActiveRecord
	And(cond string, args ...interface{}) MockableActiveRecord
	WhereAnd(conds []string, args ...interface{}) MockableActiveRecord
	Or(cond string, args ...interface{}) MockableActiveRecord
	In(vals []string, args ...interface{}) MockableActiveRecord
	InAddQuotes(vals []string, args ...interface{}) MockableActiveRecord
	OrderBy(fields ...string) MockableActiveRecord
	Asc() MockableActiveRecord
	Desc() MockableActiveRecord
	Limit(limit int) MockableActiveRecord
	Offset(offset int) MockableActiveRecord
	GroupBy(fields ...string) MockableActiveRecord
	Having(cond string, args ...interface{}) MockableActiveRecord
	Update(tables ...string) MockableActiveRecord
	Set(kv ...string) MockableActiveRecord
	Show(kv string) MockableActiveRecord
	Delete(tables ...string) MockableActiveRecord
	InsertInto(table string, fields ...string) MockableActiveRecord
	Values(vals []string, args ...interface{}) MockableActiveRecord
	AddSQL(sql string) MockableActiveRecord
	AddToken(token ...string) MockableActiveRecord
	AddArgs(args ...interface{}) MockableActiveRecord
	AddParenthesis() MockableActiveRecord
	Append(other MockableActiveRecord) MockableActiveRecord
	CleanTokens() MockableActiveRecord
	GetTokens() []string
	GetArgs() []interface{}
	Close()
	String() string
	Subquery(sub string, alias string) string
	ExecString() string
	SubAR(sub MockableActiveRecord, alias string) string
	ArgsString() string
	PrintableString() string
}
