package godatabend

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/test-go/testify/suite"
)

var (
	_ driver.Conn    = new(DatabendConn)
	_ driver.Execer  = new(DatabendConn) // nolint:staticcheck
	_ driver.Queryer = new(DatabendConn) // nolint:staticcheck
	_ driver.Tx      = new(DatabendConn)
)

var (
	_ driver.Driver = new(DatabendDriver)
)

var ddls = []string{
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

var initialzer = new(dbInit)

type dbInit struct {
	mu   sync.Mutex
	done bool
}

type databendSuit struct {
	suite.Suite
	conn *sql.DB
}

// TEST_DATABEND_DSN=https://user:password@app.databend.com:443/books?idle_timeout=1h0m0s&org=databend&warehouse=bl
func (s *databendSuit) SetupSuite() {
	dsn := os.Getenv("TEST_DATABEND_DSN")
	if len(dsn) == 0 {
		panic(fmt.Errorf("no TEST_DATABEND_DSN ENV"))
	}

	conn, err := sql.Open("databend", dsn)
	s.Require().NoError(err)
	s.Require().NoError(initialzer.Do(conn))
	s.conn = conn
}

func (d *dbInit) Do(conn *sql.DB) error {
	if d.done {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.done {
		return nil
	}
	for _, ddl := range ddls {
		if _, err := conn.Exec(ddl); err != nil {
			return err
		}
	}
	d.done = true
	return nil
}

func parseTime(layout, s string) time.Time {
	t, err := time.Parse(layout, s)
	if err != nil {
		panic(err)
	}
	return t
}

func parseDate(s string) time.Time {
	return parseTime(dateFormat, s)
}

func parseDateTime(s string) time.Time {
	return parseTime(timeFormat, s)
}

func (s *databendSuit) TearDownSuite() {
	s.conn.Close()
	_, err := s.conn.Query("SELECT 1")
	s.EqualError(err, "sql: database is closed")
}

func scanValues(rows *sql.Rows, template []interface{}) (interface{}, error) {
	var result [][]interface{}
	types := make([]reflect.Type, len(template))
	for i, v := range template {
		types[i] = reflect.TypeOf(v)
	}
	ptrs := make([]interface{}, len(types))
	var err error
	for rows.Next() {
		if err = rows.Err(); err != nil {
			return nil, err
		}
		for i, t := range types {
			ptrs[i] = reflect.New(t).Interface()
		}
		err = rows.Scan(ptrs...)
		if err != nil {
			return nil, err
		}
		values := make([]interface{}, len(types))
		for i, p := range ptrs {
			values[i] = reflect.ValueOf(p).Elem().Interface()
		}
		result = append(result, values)
	}
	return result, nil
}

type connSuite struct {
	databendSuit
}

func (s *connSuite) TestExec() {
	testCases := []struct {
		query  string
		query2 string
		args   []interface{}
	}{
		{
			"INSERT INTO data (i64) VALUES (?)",
			"SELECT i64 FROM data WHERE i64=?",
			[]interface{}{int64(1)},
		},
		{
			"INSERT INTO data (i64, u64) VALUES (?, ?)",
			"SELECT i64, u64 FROM data WHERE i64=? AND u64=?",
			[]interface{}{int64(2), uint64(12)},
		},
		{
			"INSERT INTO data (i64, a16, a8) VALUES (?, ?, ?)",
			"",
			[]interface{}{int64(3), Array([]int16{1, 2}), Array([]uint8{10, 20})},
		},
		{
			"INSERT INTO data (d, t) VALUES (?, ?)",
			"",
			[]interface{}{
				Date(time.Date(2016, 4, 4, 0, 0, 0, 0, time.Local)),
				time.Date(2016, 4, 4, 0, 0, 0, 0, time.Local),
			},
		},
	}
	for _, tc := range testCases {
		result, err := s.conn.Exec(tc.query, tc.args...)
		if !s.NoError(err) {
			continue
		}
		s.NotNil(result)
		_, err = result.LastInsertId()
		s.Equal(ErrNoLastInsertID, err)
		_, err = result.RowsAffected()
		s.Equal(ErrNoRowsAffected, err)
		if len(tc.query2) == 0 {
			continue
		}
		rows, err := s.conn.Query(tc.query2, tc.args...)
		if !s.NoError(err) {
			continue
		}
		v, err := scanValues(rows, tc.args)
		if s.NoError(err) {
			s.Equal([][]interface{}{tc.args}, v)
		}
		s.NoError(rows.Close())
	}
}

func (s *connSuite) TestServerError() {
	_, err := s.conn.Query("SELECT 1 FROM '???'")
	s.Contains(err.Error(), "error")
}

func TestConn(t *testing.T) {
	suite.Run(t, new(connSuite))
}
