//go:build !resume_query_skip
// +build !resume_query_skip

package tests

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	databend "github.com/datafuselabs/databend-go"
	"golang.org/x/mod/semver"
	"time"
)

func (s *DatabendTestSuite) TestBatchInsert() {
	if semver.Compare(driverVersion, "v0.9.0") <= 0 || semver.Compare(serverVersion, "1.2.836") < 0 {
		return
	}

	db := sql.OpenDB(s.cfg)
	defer db.Close()

	locLA, err := time.LoadLocation("America/Los_Angeles")
	s.r.NoError(err)

	tableName := "test_batch_insert"
	q := `CREATE OR REPLACE TABLE %s (
		i64 Int64,
		f64 Float64,
		s   String,
		a8  Array(UInt8),
		d   Date,
		t   DateTime
	)`
	_, err = db.Exec(fmt.Sprintf(q, tableName))
	s.r.NoError(err)
	ctx := context.Background()

	conn, err := db.Conn(context.Background())
	s.r.NoError(err)

	_, err = conn.ExecContext(ctx, fmt.Sprintf("set timezone='%s'", locLA.String()))
	s.r.NoError(err)

	today := time.Date(2021, time.January, 2, 0, 0, 0, 0, time.UTC)
	todayLA := time.Date(2021, time.January, 2, 0, 0, 0, 0, locLA)
	yesterday := time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC)
	batch := [][]driver.Value{
		{1, 1.2, "s1", "[1, 2, 3]", "2021-01-02", "2021-01-02 00:00:00"},
		{2, 2.2, "s1", []int{1, 2, 3}, today, today},
		{3, 3.2, "s1", []int{1, 2, 3}, databend.Date(today), today},
		{4, 3.2, "s1", []int{1, 2, 3}, today.In(locLA), today},
	}
	query := fmt.Sprintf("insert into %s values", tableName)
	stmt, err := databend.PrepareBatch(query)
	r, err := stmt.ExecBatch(context.Background(), conn, batch)
	s.r.NoError(err)
	n, err := r.RowsAffected()
	s.r.NoError(err)
	s.r.Equal(n, int64(len(batch)))

	//s.r.NoError(err)
	rows, err := conn.QueryContext(context.Background(), "select * from "+tableName+" order by i64")
	s.r.NoError(err)
	result, err := scanValues(rows)
	s.r.NoError(err)
	exp := [][]interface{}{
		{
			"1",
			"1.2",
			"s1",
			"[1,2,3]",
			today,
			todayLA,
		},
		{
			"2",
			"2.2",
			"s1",
			"[1,2,3]",
			today,
			today.In(locLA),
		},
		{
			"2",
			"2.2",
			"s1",
			"[1,2,3]",
			today,
			today.In(locLA),
		},
		{
			"2",
			"2.2",
			"s1",
			"[1,2,3]",
			yesterday,
			today.In(locLA),
		},
	}
	for i := 0; i < len(batch); i++ {
		println("case ", i)
		s.r.Equal(exp[0], result[0])
	}
	_ = rows.Close()
}
