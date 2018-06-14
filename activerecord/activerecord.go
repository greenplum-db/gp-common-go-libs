package activerecord

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	pq "github.com/greenplum-db/gp-golang-libpq"
)

// ActiveRecord represents a database connection and a sql string.
type ActiveRecord struct {
	DB     *sql.DB
	Tokens []string
	Args   []interface{}
}

const comma = ","
const holder = "?"

// NewActiveRecord is the factory method to create an empty ActiveRecord
func NewActiveRecord() MockableActiveRecord {
	return &ActiveRecord{}
}

// NewActiveRecordWithDB return a ActiveRecord with given sql.DB connection
func NewActiveRecordWithDB(db *sql.DB) MockableActiveRecord {
	return &ActiveRecord{DB: db}
}

// Connect connect to dbtype database by provided url.
func (ar *ActiveRecord) Connect(dbtype string, url string) (err error) {
	if ar.DB != nil {
		ar.Close()
	}

	if ar.DB, err = sql.Open(dbtype, url); err != nil {
		return err
	}
	return nil
}

// Close close the connection.
func (ar *ActiveRecord) Close() {
	if ar.DB != nil {
		err := ar.DB.Close()
		if err != nil {
			log.Println(err)
		}
	}
	ar.DB = nil
}

// Exec will execute the sql string represented by ActiveRecord.
func (ar *ActiveRecord) Exec() (sql.Result, error) {
	return ar.DB.Exec(ar.ExecString(), ar.Args...)
}

// ExecSQL execute the sql string in argument.
func (ar *ActiveRecord) ExecSQL(sql string, args ...interface{}) (sql.Result, error) {
	return ar.DB.Exec(sql, args...)
}

// GetCount get the row count of the result of ActiveRecord sql.
func (ar *ActiveRecord) GetCount(args ...interface{}) (count int, err error) {
	ar.Args = append(ar.Args, args)
	if err = ar.DB.QueryRow(ar.ExecString(), ar.Args...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// GetRows return the full execution result of ActiveRecord sql, all of the values are in interface{} type.
func (ar *ActiveRecord) GetRows() (result []map[string]interface{}, err error) {
	rows, err := ar.DB.Query(ar.ExecString(), ar.Args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := rows.Close()
		if err != nil {
			log.Println(err)
		}
	}()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	dest := make([]interface{}, len(columns))
	fields := make([]interface{}, len(columns))
	for i := range fields {
		dest[i] = &fields[i]
	}

	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}

		r := make(map[string]interface{})
		for i, v := range fields {
			if str, ok := v.(string); ok {
				r[columns[i]] = str
			} else {
				switch v.(type) {
				case time.Time:
					t := v.(time.Time)
					r[columns[i]] = t.String()[:19]
				case []uint8:
					r[columns[i]] = string(v.([]uint8))
				default:
					r[columns[i]] = v
				}
			}
		}

		result = append(result, r)
	}

	if err := rows.Err(); err != nil {
		if err == driver.ErrBadConn {
			return result, nil
		}
		return nil, err

	}
	return result, nil
}

// GetRowsI get rows as map[string]interface{}
func (ar *ActiveRecord) GetRowsI(query string, args ...interface{}) (result []map[string]interface{}, err error) {

	rows, err := ar.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := rows.Close()
		if err != nil {
			log.Println(err)
		}
	}()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	dest := make([]interface{}, len(columns))
	fields := make([]interface{}, len(columns))
	for i := range fields {
		dest[i] = &fields[i]
	}

	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}

		r := make(map[string]interface{})
		for i, value := range fields {
			switch v := value.(type) {
			case sql.NullBool:
				if v.Valid {
					r[columns[i]] = v.Bool
				} else {
					r[columns[i]] = nil
				}
			case sql.NullFloat64:
				if v.Valid {
					r[columns[i]] = v.Float64
				} else {
					r[columns[i]] = nil
				}
			case sql.NullInt64:
				if v.Valid {
					r[columns[i]] = v.Int64
				} else {
					r[columns[i]] = nil
				}
			case sql.NullString:
				if v.Valid {
					r[columns[i]] = v.String
				} else {
					r[columns[i]] = nil
				}
			case pq.NullTime:
				if v.Valid {
					r[columns[i]] = v.Time
				} else {
					r[columns[i]] = nil
				}
			case sql.RawBytes:
				r[columns[i]] = string(v)
			case nil:
				r[columns[i]] = nil
			case []uint8:
				r[columns[i]] = string(v)
			case time.Time:
				r[columns[i]] = v
			case int64:
				r[columns[i]] = v
			default:
				r[columns[i]] = v
			}
		}

		result = append(result, r)
	}

	if err := rows.Err(); err != nil {
		if err == driver.ErrBadConn {
			return result, nil
		}
		return nil, err
	}
	return result, nil
}

// GetRow return the first row of execution result of ActiveRecord sql, all of the values are in interface{} type.
func (ar *ActiveRecord) GetRow() (map[string]interface{}, error) {
	rows, err := ar.GetRows()
	if err != nil || len(rows) == 0 {
		return nil, err
	}

	return rows[0], nil
}

// Select add "select" and the given fields
func (ar *ActiveRecord) Select(fields ...string) MockableActiveRecord {
	ar.Tokens = append(ar.Tokens, "SELECT", strings.Join(fields, comma))
	return ar
}

// SelectDistinct add "SELECT DISTINCT" and the given fields
func (ar *ActiveRecord) SelectDistinct(fields ...string) MockableActiveRecord {
	ar.Tokens = append(ar.Tokens, "SELECT DISTINCT", strings.Join(fields, comma))
	return ar
}

// From add "FROM" and the given tables as token
func (ar *ActiveRecord) From(tables ...string) MockableActiveRecord {
	ar.Tokens = append(ar.Tokens, "FROM", strings.Join(tables, comma))
	return ar
}

// Join add "JOIN" and the given table name
func (ar *ActiveRecord) Join(table string) MockableActiveRecord {
	if len(table) > 0 {
		ar.Tokens = append(ar.Tokens, "JOIN", table)
	}
	return ar
}

// InnerJoin add "INNER JOIN" and the table name
func (ar *ActiveRecord) InnerJoin(table string) MockableActiveRecord {
	if len(table) > 0 {
		ar.Tokens = append(ar.Tokens, "INNER JOIN", table)
	}
	return ar
}

// LeftJoin add "LEFT JOIN" and the table name
func (ar *ActiveRecord) LeftJoin(table string) MockableActiveRecord {
	if len(table) > 0 {
		ar.Tokens = append(ar.Tokens, "LEFT JOIN", table)
	}
	return ar
}

// RightJoin add "RIGHT JOIN" and the table name
func (ar *ActiveRecord) RightJoin(table string) MockableActiveRecord {
	if len(table) > 0 {
		ar.Tokens = append(ar.Tokens, "RIGHT JOIN", table)
	}
	return ar
}

// LeftOuterJoin add "LEFT OUTER JOIN" and the table name
func (ar *ActiveRecord) LeftOuterJoin(table string) MockableActiveRecord {
	if len(table) > 0 {
		ar.Tokens = append(ar.Tokens, "LEFT OUTER JOIN", table)
	}
	return ar
}

// RightOuterJoin add "RIGHT OUTER JOIN" and the table name
func (ar *ActiveRecord) RightOuterJoin(table string) MockableActiveRecord {
	if len(table) > 0 {
		ar.Tokens = append(ar.Tokens, "RIGHT OUTER JOIN", table)
	}
	return ar
}

func (ar *ActiveRecord) appendArgs(args ...interface{}) {
	ar.Args = append(ar.Args, args...)
}

// On add "ON" and the condition string into tokens, and args into Args
func (ar *ActiveRecord) On(cond string, args ...interface{}) MockableActiveRecord {
	if len(cond) > 0 {
		ar.Tokens = append(ar.Tokens, "ON", cond)
		ar.appendArgs(args...)
	}
	return ar
}

// Where add "WHERE" and the condition to tokens, and args into Args
func (ar *ActiveRecord) Where(cond string, args ...interface{}) MockableActiveRecord {
	if len(cond) > 0 {
		ar.Tokens = append(ar.Tokens, "WHERE", cond)
		ar.appendArgs(args...)
	}
	return ar
}

// And add "AND" and condition into token, and args to Args
func (ar *ActiveRecord) And(cond string, args ...interface{}) MockableActiveRecord {
	if len(cond) > 0 {
		ar.Tokens = append(ar.Tokens, "AND", cond)
		ar.appendArgs(args...)
	}
	return ar
}

// WhereAnd add conditions into token and args into Args
func (ar *ActiveRecord) WhereAnd(conds []string, args ...interface{}) MockableActiveRecord {
	firstNotEmptyFound := false
	ar.appendArgs(args...)
	for _, cond := range conds {
		if len(cond) > 0 {
			if !firstNotEmptyFound {
				ar.Tokens = append(ar.Tokens, "WHERE", cond)
				firstNotEmptyFound = true
				continue
			} else {
				ar.Tokens = append(ar.Tokens, "AND", cond)
			}
		}
	}
	return ar
}

// Or add "OR" and condition into tokens, and args into Args
func (ar *ActiveRecord) Or(cond string, args ...interface{}) MockableActiveRecord {
	if len(cond) > 0 {
		ar.Tokens = append(ar.Tokens, "OR", cond)
		ar.appendArgs(args...)
	}
	return ar
}

// In add "IN" and values into token and args into Args
func (ar *ActiveRecord) In(vals []string, args ...interface{}) MockableActiveRecord {
	cond := strings.Join(vals, comma)
	ar.Tokens = append(ar.Tokens, "IN", "(", cond, ")")
	ar.appendArgs(args...)
	return ar
}

// InAddQuotes is the same as In but the values are quoted with ''
func (ar *ActiveRecord) InAddQuotes(vals []string, args ...interface{}) MockableActiveRecord {
	qvals := []string{}
	for _, val := range vals {
		qvals = append(qvals, fmt.Sprintf("'%s'", val))
	}
	ar.AddArgs(args)
	return ar.In(qvals)
}

// OrderBy add "ORDER BY" and the fields joined with comma
func (ar *ActiveRecord) OrderBy(fields ...string) MockableActiveRecord {
	ar.Tokens = append(ar.Tokens, "ORDER BY", strings.Join(fields, comma))
	return ar
}

// Asc add "ASC" in token, should be used with OrderBy
func (ar *ActiveRecord) Asc() MockableActiveRecord {
	ar.Tokens = append(ar.Tokens, "ASC")
	return ar
}

// Desc add "DESC" in token, should be used with OrderBy
func (ar *ActiveRecord) Desc() MockableActiveRecord {
	ar.Tokens = append(ar.Tokens, "DESC")
	return ar
}

// Limit add "LIMIT" and the number in tokens
func (ar *ActiveRecord) Limit(limit int) MockableActiveRecord {
	ar.Tokens = append(ar.Tokens, "LIMIT", strconv.Itoa(limit))
	return ar
}

// Offset add "OFFSET" and the number in tokens
func (ar *ActiveRecord) Offset(offset int) MockableActiveRecord {
	ar.Tokens = append(ar.Tokens, "OFFSET", strconv.Itoa(offset))
	return ar
}

// GroupBy add "GROUP BY" and the fileds into tokens
func (ar *ActiveRecord) GroupBy(fields ...string) MockableActiveRecord {
	ar.Tokens = append(ar.Tokens, "GROUP BY", strings.Join(fields, comma))
	return ar
}

// Having add the token "HAVING" and the conditions into tokens, args into Args
func (ar *ActiveRecord) Having(cond string, args ...interface{}) MockableActiveRecord {
	ar.Tokens = append(ar.Tokens, "HAVING", cond)
	ar.appendArgs(args...)
	return ar
}

// Update add the token "UPDATE" and the tables name into tokens
func (ar *ActiveRecord) Update(tables ...string) MockableActiveRecord {
	ar.Tokens = append(ar.Tokens, "UPDATE", strings.Join(tables, comma))
	return ar
}

// Set add the token "SET" and the key values
func (ar *ActiveRecord) Set(kv ...string) MockableActiveRecord {
	ar.Tokens = append(ar.Tokens, "SET", strings.Join(kv, comma))
	return ar
}

// Show add "SHOW" and key value sting
func (ar *ActiveRecord) Show(kv string) MockableActiveRecord {
	ar.Tokens = append(ar.Tokens, "Show", kv)
	return ar
}

// Delete add "DELETE" and the tables into tokens
func (ar *ActiveRecord) Delete(tables ...string) MockableActiveRecord {
	ar.Tokens = append(ar.Tokens, "DELETE")
	if len(tables) != 0 {
		ar.Tokens = append(ar.Tokens, strings.Join(tables, comma))
	}
	return ar
}

// InsertInto add "INSERT INTO" and the table into tokens
func (ar *ActiveRecord) InsertInto(table string, fields ...string) MockableActiveRecord {
	ar.Tokens = append(ar.Tokens, "INSERT INTO", table)
	if len(fields) != 0 {
		fieldsStr := strings.Join(fields, comma)
		ar.Tokens = append(ar.Tokens, "(", fieldsStr, ")")
	}
	return ar
}

// Values add "VALUES" and the options values into token
func (ar *ActiveRecord) Values(vals []string, args ...interface{}) MockableActiveRecord {
	valsStr := strings.Join(vals, comma)
	ar.Tokens = append(ar.Tokens, "VALUES", "(", valsStr, ")")
	ar.appendArgs(args...)
	return ar
}

// AddSQL add the given sql into token
func (ar *ActiveRecord) AddSQL(sql string) MockableActiveRecord {
	ar.Tokens = append(ar.Tokens, sql)
	return ar
}

// Subquery combine a sub query and its alias into (sql) as alias format
func (ar *ActiveRecord) Subquery(sub string, alias string) string {
	return fmt.Sprintf("(%s) AS %s", sub, alias)
}

// SubAR add add another ar as sub ar
func (ar *ActiveRecord) SubAR(sub MockableActiveRecord, alias string) string {
	ar.Args = append(ar.Args, sub.GetArgs()...)
	return fmt.Sprintf("(%s) AS %s", sub.String(), alias)
}

// ExecString return the sql string represented by Tokens.
func (ar *ActiveRecord) ExecString() string {
	str := strings.Join(ar.Tokens, " ")
	if len(ar.Args) > 0 {
		for i := range ar.Args {
			str = strings.Replace(str, holder, fmt.Sprintf("$%d", i+1), 1)
		}
	}
	return str
}

// String print all the tokens
func (ar *ActiveRecord) String() string {
	return strings.Join(ar.Tokens, " ")
}

// ArgsString print all the arguments
func (ar *ActiveRecord) ArgsString() string {
	str := fmt.Sprintf("total length %d\n", len(ar.Args))
	for i, arg := range ar.Args {
		str = str + fmt.Sprintf("$%d:%v ", i, arg)
	}
	return str
}

// PrintableString print the sql and arguments
func (ar *ActiveRecord) PrintableString() string {
	stoken := strings.Join(ar.Tokens, " ")
	return fmt.Sprintf("%s\n[%v]\n", stoken, ar.Args)
}

// CleanTokens clean all tokens
func (ar *ActiveRecord) CleanTokens() MockableActiveRecord {
	ar.Tokens = ar.Tokens[:0]
	ar.Args = ar.Args[:0]
	return ar
}

// AddToken append token after the existing Tokens.
func (ar *ActiveRecord) AddToken(token ...string) MockableActiveRecord {
	ar.Tokens = append(ar.Tokens, token...)
	return ar
}

// AddArgs append args to ar.Args
func (ar *ActiveRecord) AddArgs(args ...interface{}) MockableActiveRecord {
	ar.Args = append(ar.Args, args...)
	return ar
}

// AddParenthesis (all the tokens)
func (ar *ActiveRecord) AddParenthesis() MockableActiveRecord {
	ar.Tokens = append([]string{"("}, ar.Tokens...)
	ar.Tokens = append(ar.Tokens, ")")
	return ar
}

// Append append another ActiveRecord's Token after the existing Tokens.
func (ar *ActiveRecord) Append(other MockableActiveRecord) MockableActiveRecord {
	ar.Tokens = append(ar.Tokens, other.GetTokens()...)
	ar.Args = append(ar.Args, other.GetArgs()...)
	return ar
}

// GetTokens return all ar's Tokens
func (ar *ActiveRecord) GetTokens() []string {
	return ar.Tokens
}

// GetArgs return all ar's args
func (ar *ActiveRecord) GetArgs() []interface{} {
	return ar.Args
}
