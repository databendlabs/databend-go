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
		f64 Float64
	)`
	_, err := db.Exec(fmt.Sprintf(q, tableName))
	s.r.NoError(err)

	conn, err := db.Conn(context.Background())
	s.r.NoError(err)

	batch := [][]driver.Value{
		{int64(1), 1.2},
		{int64(2), 2.2},
	}

	err = conn.Raw(func(rawConn interface{}) error {
		bendConn := rawConn.(*godatabend.DatabendConn)
		query := fmt.Sprintf("insert into %s values", tableName)
		r, err := bendConn.ExecBatch(context.Background(), query, batch)
		if err != nil {
			return err
		}
		n, err := r.RowsAffected()
		s.r.Equal(n, int64(2))
		if err != nil {
			return err
		}
		return nil
	})
	s.r.NoError(err)

	rows, err := db.Query("select * from " + tableName)
	s.r.NoError(err)
	result, err := scanValues(rows)
	s.r.NoError(err)
	exp := [][]interface{}{{"1", "1.2"}, {"2", "2.2"}}
	s.r.Equal(exp, result)
	_ = rows.Close()

}
