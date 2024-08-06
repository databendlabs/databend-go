package tests

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/test-go/testify/suite"

	dc "github.com/datafuselabs/databend-go"
)

const (
	createTable = `CREATE TABLE %s (
		i64 Int64,
		u64 UInt64,
		f64 Float64,
		s   String,
		s2  String,
		a16 Array(Int16),
		a8  Array(UInt8),
		d   Date,
		t   DateTime)`
	createTable2 = `create table %s (a string);`
)

var (
	dsn = "http://databend:databend@localhost:8000?presign=true"
)

func init() {
	dsn = os.Getenv("TEST_DATABEND_DSN")
	// databend default
	// dsn = "http://root:@localhost:8000?presign=true"

	// add user databend by uncommenting corresponding [[query.users]] section scripts/ci/deploy/config/databend-query-node-1.toml
	//dsn = "http://databend:databend@localhost:8000?presign=true"
}

func TestDatabendSuite(t *testing.T) {
	suite.Run(t, new(DatabendTestSuite))
}

type DatabendTestSuite struct {
	suite.Suite
	db     *sql.DB
	table  string
	table2 string
	r      *require.Assertions
}

func (s *DatabendTestSuite) SetupSuite() {
	var err error

	s.NotEmpty(dsn)
	s.db, err = sql.Open("databend", dsn)
	s.Nil(err)

	err = s.db.Ping()
	s.Nil(err)

	rows, err := s.db.Query("select version()")
	s.Nil(err)
	result, err := scanValues(rows)
	s.Nil(err)

	s.T().Logf("connected to databend: %s\n", result)
}

func (s *DatabendTestSuite) TearDownSuite() {
	_ = s.db.Close()
}

func (s *DatabendTestSuite) SetupTest() {
	t := s.T()
	s.r = require.New(t)

	s.table = fmt.Sprintf("test_%s_%d", t.Name(), time.Now().Unix())
	// t.Logf("setup test with table %s", s.table)
	s.table2 = fmt.Sprintf("test_%s_%d", t.Name(), time.Now().Unix()+1)

	_, err := s.db.Exec(fmt.Sprintf(createTable, s.table))
	s.r.Nil(err)
	_, err = s.db.Exec(fmt.Sprintf(createTable2, s.table2))
	s.r.Nil(err)
}

func (s *DatabendTestSuite) TearDownTest() {
	// t := s.T()
	s.SetupSuite()

	// t.Logf("teardown test with table %s", s.table)
	_, err := s.db.Exec(fmt.Sprintf("DROP TABLE %s", s.table))
	s.r.Nil(err)
	_, err = s.db.Exec(fmt.Sprintf("DROP TABLE %s", s.table2))
	s.r.Nil(err)
}

// For load balance test
func (s *DatabendTestSuite) TestCycleExec() {
	rows, err := s.db.Query("SELECT number from numbers(200) order by number")
	s.r.Nil(err)
	_, err = scanValues(rows)
	s.r.Nil(err)
}

func (s *DatabendTestSuite) TestQuoteStringQuery() {
	m := make(map[string]string, 0)
	m["message"] = "this is action 'with quote string'"
	x, err := json.Marshal(m)
	s.r.Nil(err)
	_, err = s.db.Exec(fmt.Sprintf("insert into %s values(?)", s.table2), string(x))
	s.r.Nil(err)
	rows, err := s.db.Query(fmt.Sprintf("select * from %s", s.table2))
	s.r.Nil(err)
	for rows.Next() {
		var t string
		_ = rows.Scan(&t)
		s.r.Equal(string(x), t)
	}
}

func (s *DatabendTestSuite) TestDesc() {
	rows, err := s.db.Query("DESC " + s.table)
	s.r.Nil(err)

	result, err := scanValues(rows)
	s.r.Nil(err)
	s.r.Equal([][]interface{}{{"i64", "BIGINT", "YES", "NULL", ""}, {"u64", "BIGINT UNSIGNED", "YES", "NULL", ""}, {"f64", "DOUBLE", "YES", "NULL", ""}, {"s", "VARCHAR", "YES", "NULL", ""}, {"s2", "VARCHAR", "YES", "NULL", ""}, {"a16", "ARRAY(INT16)", "YES", "NULL", ""}, {"a8", "ARRAY(UINT8)", "YES", "NULL", ""}, {"d", "DATE", "YES", "NULL", ""}, {"t", "TIMESTAMP", "YES", "NULL", ""}}, result)
	_ = rows.Close()
}

func (s *DatabendTestSuite) TestBasicSelect() {
	query := "SELECT ?"
	result, err := s.db.Exec(query, []interface{}{1}...)
	s.r.Nil(err)

	affected, err := result.RowsAffected()
	s.r.Nil(err)
	// s.r.ErrorIs(err, dc.ErrNoRowsAffected)
	s.r.Equal(int64(0), affected)
}

func (s *DatabendTestSuite) TestSelectMultiPage() {
	// by default, each page size is 10000 rows
	// So we need a large result set to test the multi pages case.
	n := 46000
	query := fmt.Sprintf("SELECT number from numbers(%d) order by number", n)
	rows, err := s.db.Query(query)
	s.r.Nil(err)

	v := -1
	for i := 0; i < n; i++ {
		s.r.True(rows.Next())
		rows.Scan(&v)
		s.r.Equal(v, i)
	}
	s.r.False(rows.Next())
}

func (s *DatabendTestSuite) TestBatchInsert() {
	r := require.New(s.T())

	scope, err := s.db.Begin()
	r.Nil(err)

	batch, err := scope.Prepare(fmt.Sprintf("INSERT INTO %s VALUES", s.table))
	r.Nil(err)

	for i := 0; i < 10; i++ {
		_, err = batch.Exec(
			"1234",
			"2345",
			"3.1415",
			"test",
			"test2",
			"[4, 5, 6]",
			"[1, 2, 3]",
			"2021-01-01",
			"2021-01-01 00:00:00",
		)
		r.Nil(err)
	}

	err = scope.Commit()
	r.Nil(err)
}

func (s *DatabendTestSuite) TestDDL() {
	ddls := []string{
		`DROP TABLE IF EXISTS data`,
		`CREATE TABLE data (
				i64 Int64,
				u64 UInt64,
				f64 Float64,
				s   String,
				s2  String,
				a16 Array(Int16),
				a8  Array(UInt8),
				d   Date,
				t   DateTime)
		`,
		`INSERT INTO data VALUES
			(-1, 1, 1.0, '1', '1', [1], [10], '2011-03-06', '2011-03-06 06:20:00'),
			(-2, 2, 2.0, '2', '2', [2], [20], '2012-05-31', '2012-05-31 11:20:00'),
			(-3, 3, 3.0, '3', '2', [3], [30], '2016-04-04', '2016-04-04 11:30:00')
		`,
	}
	for _, ddl := range ddls {
		_, err := s.db.Exec(ddl)
		s.Nil(err)
	}
}

func (s *DatabendTestSuite) TestExec() {
	testCases := []struct {
		query  string
		query2 string
		args   []interface{}
	}{
		{
			fmt.Sprintf("INSERT INTO %s (i64) VALUES (?)", s.table),
			"",
			[]interface{}{int64(1)},
		},
		{
			fmt.Sprintf("INSERT INTO %s (i64, u64) VALUES (?, ?)", s.table),
			"",
			[]interface{}{int64(2), uint64(12)},
		},
		{
			fmt.Sprintf("INSERT INTO %s (i64, a16, a8) VALUES (?, ?, ?)", s.table),
			"",
			[]interface{}{int64(3), dc.Array([]int16{1, 2}), dc.Array([]uint8{10, 20})},
		},
		{
			fmt.Sprintf("INSERT INTO %s (d, t) VALUES (?, ?)", s.table),
			"",
			[]interface{}{
				dc.Date(time.Date(2016, 4, 4, 0, 0, 0, 0, time.Local)),
				time.Date(2016, 4, 4, 0, 0, 0, 0, time.Local),
			},
		},
	}
	for _, tc := range testCases {
		result, err := s.db.Exec(tc.query, tc.args...)
		s.T().Logf("query: %s, args: %v\n", tc.query, tc.args)
		s.r.Nil(err)
		s.r.NotNil(result)

		if len(tc.query2) == 0 {
			continue
		}
		rows, err := s.db.Query(tc.query2, tc.args...)
		s.r.Nil(err)

		v, err := scanValues(rows)
		s.r.Nil(err)
		s.r.Equal([][]interface{}{tc.args}, v)

		s.r.NoError(rows.Close())
	}
}

func (s *DatabendTestSuite) TestServerError() {
	_, err := s.db.Query("SELECT 1 FROM '???'")
	s.Contains(err.Error(), "error")
}

func (s *DatabendTestSuite) TestQueryNull() {
	rows, err := s.db.Query("SELECT NULL")
	s.r.Nil(err)

	result, err := scanValues(rows)
	s.r.Nil(err)
	s.r.Equal([][]interface{}{{"NULL"}}, result)

	s.r.NoError(rows.Close())
}

func (s *DatabendTestSuite) TestTransactionCommit() {
	tx, err := s.db.Begin()
	s.r.Nil(err)

	_, err = tx.Exec(fmt.Sprintf("INSERT INTO %s (i64) VALUES (?)", s.table), int64(1))
	s.r.Nil(err)

	err = tx.Commit()
	s.r.Nil(err)

	rows, err := s.db.Query(fmt.Sprintf("SELECT * FROM %s", s.table))
	s.r.Nil(err)

	result, err := scanValues(rows)
	s.r.Nil(err)
	s.r.Equal([][]interface{}{{1, nil, nil, "NULL", "NULL", nil, nil, nil, nil}}, result)

	s.r.NoError(rows.Close())
}

func (s *DatabendTestSuite) TestTransactionRollback() {
	tx, err := s.db.Begin()
	s.r.Nil(err)

	_, err = tx.Exec(fmt.Sprintf("INSERT INTO %s (i64) VALUES (?)", s.table), int64(1))
	s.r.Nil(err)
	rows, err := s.db.Query(fmt.Sprintf("SELECT * FROM %s", s.table))
	s.r.Nil(err)

	result, err := scanValues(rows)
	s.r.Nil(err)
	s.r.Equal([][]interface{}(nil), result)

	err = tx.Rollback()
	s.r.Nil(err)

	rows, err = s.db.Query(fmt.Sprintf("SELECT * FROM %s", s.table))
	s.r.Nil(err)

	result, err = scanValues(rows)
	s.r.Nil(err)
	s.r.Empty(result)

	s.r.NoError(rows.Close())
}

func (s *DatabendTestSuite) TestLongExec() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := s.db.ExecContext(ctx, "SELECT number from numbers(100000) order by number")
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			s.T().Errorf("Query execution exceeded the 10s timeout")
		} else {
			s.r.Nil(err)
		}
	}
}

func getNullableType(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Ptr {
		return t.Elem()
	}
	return t
}

func scanValues(rows *sql.Rows) (interface{}, error) {
	var err error
	var result [][]interface{}
	ct, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	vals := make([]any, len(ct))
	for rows.Next() {
		if err = rows.Err(); err != nil {
			return nil, err
		}
		for i := range ct {
			vals[i] = &dc.NullableValue{}
		}
		err = rows.Scan(vals...)
		if err != nil {
			return nil, err
		}
		values := make([]interface{}, len(ct))
		for i, p := range vals {
			val, err := p.(*dc.NullableValue).Value()
			if err != nil {
				return nil, fmt.Errorf("failed to get value: %w", err)
			}
			values[i] = val
		}
		result = append(result, values)
	}
	return result, nil
}
