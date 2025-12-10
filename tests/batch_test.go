//go:build !resume_query_skip
// +build !resume_query_skip

package tests

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	godatabend "github.com/datafuselabs/databend-go"
	"golang.org/x/mod/semver"
	"time"
)

func (s *DatabendTestSuite) TestBatchInsert() {
	if semver.Compare(driverVersion, "v0.9.0") < 0 {
		return
	}

	db := sql.OpenDB(s.cfg)
	defer db.Close()

	tableName := "test_batch_insert"
	q := `CREATE OR REPLACE TABLE %s (
		i64 Int64,
		f64 Float64,
		s   String,
		a8  Array(UInt8),
		d   Date,
		t   DateTime
	)`
	_, err := db.Exec(fmt.Sprintf(q, tableName))
	s.r.NoError(err)

	rs, err := db.Query("select value from system.settings where name='timezone'")
	s.r.NoError(err)
	rs.Next()
	var tz string
	rs.Scan(&tz)
	println("timezone for TestBatchInsert is:", tz)

	conn, err := db.Conn(context.Background())
	s.r.NoError(err)

	time1 := time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC)
	batch := [][]driver.Value{
		{1, 1.2, "s1", "[1, 2, 3]", "2021-01-01", "2021-01-01 00:00:00"},
		{2, 2.2, "s1", []int{1, 2, 3}, time1, time1},
	}
	query := fmt.Sprintf("insert into %s values", tableName)
	stmt, err := godatabend.PrepareBatch(query)
	r, err := stmt.ExecBatch(context.Background(), conn, batch)
	s.r.NoError(err)
	n, err := r.RowsAffected()
	s.r.NoError(err)
	s.r.Equal(n, int64(2))

	rows, err := db.Query("select * from " + tableName + " where i64 = 2")
	s.r.NoError(err)
	result, err := scanValues(rows)
	s.r.NoError(err)
	exp := [][]interface{}{{
		"2",
		"2.2",
		"s1",
		"[1,2,3]",
		time1,
		time1,
	}}

	s.r.Equal(exp, result)
	_ = rows.Close()
}
