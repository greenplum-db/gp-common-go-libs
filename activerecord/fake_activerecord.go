package activerecord

import "database/sql"

// FakeActiveRecord is only for test. You can change MockGetRows to return your own data
type FakeActiveRecord struct {
	*ActiveRecord
	MockGetRows func() ([]map[string]interface{}, error)
	MockGetRow  func(tokens []string) (map[string]interface{}, error)
	MockExecSQL func(sql string, args ...interface{}) (sql.Result, error)
}

// NewFakeActiveRecord return a *FakeActiveRecord
func NewFakeActiveRecord() *FakeActiveRecord {
	return &FakeActiveRecord{&ActiveRecord{}, func() ([]map[string]interface{}, error) {
		return nil, nil
	}, func(tokens []string) (map[string]interface{}, error) {
		return nil, nil
	}, func(sql string, args ...interface{}) (sql.Result, error) {
		return nil, nil
	}}
}

// GetRows will call MockGetRows in MockActiveRecord if it is set
func (m *FakeActiveRecord) GetRows() ([]map[string]interface{}, error) {
	if m.MockGetRows != nil {
		return m.MockGetRows()
	}
	return nil, nil
}

// GetRow will call MockGetRow in MockActiveRecord if it is set
func (m *FakeActiveRecord) GetRow() (map[string]interface{}, error) {
	if m.MockGetRow != nil {
		return m.MockGetRow(m.Tokens)
	}
	return nil, nil
}

// ExecSQL will call MockExecSQL if it is set
func (m *FakeActiveRecord) ExecSQL(sql string, args ...interface{}) (sql.Result, error) {
	if m.MockExecSQL != nil {
		return m.MockExecSQL(sql, args)
	}
	return nil, nil
}
