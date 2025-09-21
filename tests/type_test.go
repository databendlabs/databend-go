package tests

import (
	"database/sql"
	"fmt"
	dc "github.com/datafuselabs/databend-go"
	"time"
)

func (s *DatabendTestSuite) TestTypes2() {
	columns := "i64, d, t"
	input := Table1{
		I64:  164,
		Date: time.Date(2016, 4, 4, 0, 0, 0, 0, time.UTC),
		Time: time.Date(2025, 1, 16, 2, 1, 26, 739219000, time.UTC),
	}

	db := sql.OpenDB(s.cfg)
	defer db.Close()

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (?, ?, ?)", s.table, columns)
	result, err := db.Exec(insertSQL,
		input.I64,
		dc.Date(input.Date),
		input.Time,
	)
	s.r.NoError(err)
	n, err := result.RowsAffected()
	s.r.NoError(err)
	s.r.Equal(int64(1), n)

	selectSQL := fmt.Sprintf("select %s from %s", columns, s.table)
	rows, err := db.Query(selectSQL)
	s.r.NoError(err)
	s.r.True(rows.Next())
	s.r.NoError(err)
	output := Table1{}
	err = rows.Scan(&output.I64, &output.Date, &output.Time)
	s.r.NoError(err)

	s.r.Equal(input, output)

	s.r.NoError(rows.Close())
}
