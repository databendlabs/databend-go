package tests

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

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

type Table1 struct {
	I64     int64
	Int64   uint64
	Float64 float64
	String  string
	A8      []int8
	Date    time.Time
	Time    time.Time
}

var (
	dsn           = "http://root@localhost:8000?presign=on"
	driverVersion = ""
	serverVersion = ""
)

func init() {
	s := os.Getenv("TEST_DATABEND_DSN")
	if s != "" {
		dsn = s
	}

	serverVersion = getVersion("DATABEND_VERSION")
	driverVersion = getVersion("DATABEND_GO_VERSION")

	// databend default
	// dsn = "http://root:@localhost:8000?presign=on"

	// add user databend by uncommenting corresponding [[query.users]] section scripts/ci/deploy/config/databend-query-node-1.toml
	//dsn = "http://databend:databend@localhost:8000?presign=on"
}

func getVersion(name string) string {
	v := os.Getenv(name)
	if v == "" || v == "nightly" {
		v = "v100.0.0"
	}
	if !strings.HasPrefix(v, "v") {
		v = fmt.Sprintf("v%s", v)
	}
	return v
}

func TestDatabendSuite(t *testing.T) {
	suite.Run(t, new(DatabendTestSuite))
}

type DatabendTestSuite struct {
	suite.Suite
	cfg          *dc.Config
	table        string
	table2       string
	replaceTable string
	r            *require.Assertions
}

func (s *DatabendTestSuite) SetupSuite() {
	var err error

	s.NotEmpty(dsn)

	s.cfg, err = dc.ParseDSN(dsn)
	s.NoError(err)

	db, err := sql.Open("databend", dsn)
	s.NoError(err)

	s.NoError(db.Ping())
	s.NoError(db.Close())
}

func (s *DatabendTestSuite) Conn() *sql.Conn {
	db := sql.OpenDB(s.cfg)
	conn, err := db.Conn(context.Background())
	s.NoError(err)
	return conn
}

func (s *DatabendTestSuite) SetupTest() {
	t := s.T()
	s.r = require.New(t)
	db := sql.OpenDB(s.cfg)
	defer db.Close()
	tName := strings.ReplaceAll(t.Name(), "/", "__")

	s.table = fmt.Sprintf("test_%s_%d", tName, time.Now().Unix())
	// t.Logf("setup test with table %s", s.table)
	s.table2 = fmt.Sprintf("test_%s_%d", tName, time.Now().Unix()+1)
	s.replaceTable = fmt.Sprintf("test_%s_%d", tName, time.Now().Unix()+2)

	_, err := db.Exec(fmt.Sprintf(createTable, s.table))
	s.r.NoError(err)
	_, err = db.Exec(fmt.Sprintf(createTable2, s.table2))
	s.r.NoError(err)
	_, err = db.Exec(fmt.Sprintf(createTable, s.replaceTable))
	s.r.NoError(err)
}

func (s *DatabendTestSuite) TearDownTest() {
	// t := s.T()
	s.SetupSuite()
	db := sql.OpenDB(s.cfg)
	defer db.Close()

	// t.Logf("teardown test with table %s", s.table)
	_, err := db.Exec(fmt.Sprintf("DROP TABLE %s", s.table))
	s.r.NoError(err)
	_, err = db.Exec(fmt.Sprintf("DROP TABLE %s", s.table2))
	s.r.NoError(err)
	_, err = db.Exec(fmt.Sprintf("DROP TABLE %s", s.replaceTable))
	s.r.NoError(err)
}

func (s *DatabendTestSuite) TestVersion() {
	s.SetupSuite()
	db := sql.OpenDB(s.cfg)
	defer db.Close()

	rows, err := db.Query("select version()")
	s.r.NoError(err)
	result, err := scanValues(rows)
	s.r.NoError(err)
	s.T().Logf("connected to databend: %s\n", result)
}

// For load balance test
func (s *DatabendTestSuite) TestCycleExec() {
	db := sql.OpenDB(s.cfg)
	defer db.Close()
	rows, err := db.Query("SELECT number from numbers(200) order by number")
	s.r.NoError(err)
	_, err = scanValues(rows)
	s.r.NoError(err)
}

func (s *DatabendTestSuite) TestQuoteStringQuery() {
	db := sql.OpenDB(s.cfg)
	defer db.Close()
	m := make(map[string]string, 0)
	m["message"] = "this is action 'with quote string'"
	x, err := json.Marshal(m)
	quotedString := string(x)
	s.r.NoError(err)
	_, err = db.Exec(fmt.Sprintf("insert into %s values(?)", s.table2), quotedString)
	s.r.NoError(err)
	rows, err := db.Query(fmt.Sprintf("select * from %s", s.table2))
	s.r.NoError(err)

	s.r.True(rows.Next())
	var t string
	_ = rows.Scan(&t)
	s.r.Equal(quotedString, t)
}

func (s *DatabendTestSuite) TestDesc() {
	db := sql.OpenDB(s.cfg)
	defer db.Close()
	rows, err := db.Query("DESC " + s.table)
	s.r.NoError(err)

	result, err := scanValues(rows)
	s.r.NoError(err)
	s.r.Equal([][]interface{}{{"i64", "BIGINT", "YES", "NULL", ""}, {"u64", "BIGINT UNSIGNED", "YES", "NULL", ""}, {"f64", "DOUBLE", "YES", "NULL", ""}, {"s", "VARCHAR", "YES", "NULL", ""}, {"s2", "VARCHAR", "YES", "NULL", ""}, {"a16", "ARRAY(INT16)", "YES", "NULL", ""}, {"a8", "ARRAY(UINT8)", "YES", "NULL", ""}, {"d", "DATE", "YES", "NULL", ""}, {"t", "TIMESTAMP", "YES", "NULL", ""}}, result)
	_ = rows.Close()
}

func (s *DatabendTestSuite) TestBasicSelect() {
	db := sql.OpenDB(s.cfg)
	defer db.Close()
	query := "SELECT ?"
	result, err := db.Exec(query, 1)
	s.r.NoError(err)

	affected, err := result.RowsAffected()
	s.r.NoError(err)
	s.r.Equal(int64(0), affected)
}

func (s *DatabendTestSuite) TestSelectMultiPage() {
	// by default, each page size is 10000 rows
	// So we need a large result set to test the multi pages case.
	db := sql.OpenDB(s.cfg)
	defer db.Close()
	n := 46000
	query := fmt.Sprintf("SELECT number from numbers(%d) order by number", n)
	rows, err := db.Query(query)
	s.r.NoError(err)

	v := -1
	for i := 0; i < n; i++ {
		s.r.True(rows.Next())
		s.r.NoError(rows.Scan(&v))
		s.r.Equal(v, i)
	}
	s.r.False(rows.Next())
}

func (s *DatabendTestSuite) TestBatchInsert() {
	r := require.New(s.T())
	db := sql.OpenDB(s.cfg)
	defer db.Close()

	scope, err := db.Begin()
	r.NoError(err)

	batch, err := scope.Prepare(fmt.Sprintf("INSERT INTO %s VALUES", s.table))
	r.NoError(err)

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
		r.NoError(err)
	}

	err = scope.Commit()
	r.NoError(err)
}

func (s *DatabendTestSuite) TestBatchReplaceInto() {
	r := require.New(s.T())
	db := sql.OpenDB(s.cfg)
	defer db.Close()

	scope, err := db.Begin()
	r.NoError(err)

	batch, err := scope.Prepare(fmt.Sprintf("REPLACE INTO %s ON(i64) VALUES", s.replaceTable))
	r.NoError(err)

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
		r.NoError(err)
	}

	err = scope.Commit()
	r.NoError(err)
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
	db := sql.OpenDB(s.cfg)
	defer db.Close()
	for _, ddl := range ddls {
		_, err := db.Exec(ddl)
		s.NoError(err)
	}
	rows, err := db.Query(`SELECT u64 from data where i64=?`, -3)
	s.r.NoError(err)
	s.r.True(rows.Next())
	var r int32
	s.r.NoError(rows.Scan(&r))
	s.r.Equal(int32(3), r)
}

func (s *DatabendTestSuite) TestExec() {
	testCases := []struct {
		insertQuery string
		query2      string
		args        []interface{}
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
	db := sql.OpenDB(s.cfg)
	defer db.Close()
	for _, tc := range testCases {
		result, err := db.Exec(tc.insertQuery, tc.args...)
		s.T().Logf("query: %s, args: %v\n", tc.insertQuery, tc.args)
		s.r.NoError(err)
		s.r.NotNil(result)
		n, _ := result.RowsAffected()
		s.r.Equal(int64(1), n)

		if len(tc.query2) == 0 {
			continue
		}
		rows, err := db.Query(tc.query2, tc.args...)
		s.r.NoError(err)

		v, err := scanValues(rows)
		s.r.NoError(err)
		s.r.Equal([][]interface{}{tc.args}, v)

		s.r.NoError(rows.Close())
	}
}

func (s *DatabendTestSuite) TestServerError() {
	db := sql.OpenDB(s.cfg)
	defer db.Close()
	_, err := db.Query("SELECT 1 FROM '???'")
	s.Contains(err.Error(), "error")
}

func (s *DatabendTestSuite) TestTransactionCommit() {
	db := sql.OpenDB(s.cfg)
	defer db.Close()
	tx, err := db.Begin()
	s.r.NoError(err)

	_, err = tx.Exec(fmt.Sprintf("INSERT INTO %s (i64) VALUES (?)", s.table), int64(1))
	s.r.NoError(err)

	err = tx.Commit()
	s.r.NoError(err)

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s", s.table))
	s.r.NoError(err)

	_, err = scanValues(rows)
	s.r.NoError(err)

	s.r.NoError(rows.Close())
}

func (s *DatabendTestSuite) TestTransactionRollback() {
	db := sql.OpenDB(s.cfg)
	defer db.Close()
	tx, err := db.Begin()
	s.r.NoError(err)

	_, err = tx.Exec(fmt.Sprintf("INSERT INTO %s (i64) VALUES (?)", s.table), int64(1))
	s.r.NoError(err)
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s", s.table))
	s.r.NoError(err)

	result, err := scanValues(rows)
	s.r.NoError(err)
	s.r.Equal([][]interface{}(nil), result)

	err = tx.Rollback()
	s.r.NoError(err)

	rows, err = db.Query(fmt.Sprintf("SELECT * FROM %s", s.table))
	s.r.NoError(err)

	result, err = scanValues(rows)
	s.r.NoError(err)
	s.r.Empty(result)

	s.r.NoError(rows.Close())
}

func (s *DatabendTestSuite) TestLongExec() {
	db := sql.OpenDB(s.cfg)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, "SELECT number from numbers(100000) order by number")
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			s.T().Errorf("Query execution exceeded the 10s timeout")
		} else {
			s.r.NoError(err)
		}
	}
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
		for i := range ct {
			vals[i] = &NullableValue{}
		}
		err = rows.Scan(vals...)
		if err != nil {
			return nil, err
		}
		values := make([]interface{}, len(ct))
		for i, p := range vals {
			val, err := p.(*NullableValue).Value()
			if err != nil {
				return nil, fmt.Errorf("failed to get value: %w", err)
			}
			values[i] = val
		}
		result = append(result, values)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

type NullableValue struct {
	val any
}

// Scan implements the [Scanner] interface.
func (nv *NullableValue) Scan(value any) error {
	nv.val = value
	return nil
}

// Value implements the [driver.Valuer] interface.
func (nv NullableValue) Value() (driver.Value, error) {
	return nv.val, nil
}
