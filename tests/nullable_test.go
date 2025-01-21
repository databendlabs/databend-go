package tests

import (
	"context"
	"database/sql"
	"fmt"
)

func (s *DatabendTestSuite) TestNullable() {
	_, err := s.db.Exec(fmt.Sprintf("INSERT INTO %s (i64) VALUES (?)", s.table), int64(1))
	s.r.Nil(err)

	rows, err := s.db.Query(fmt.Sprintf("SELECT * FROM %s", s.table))
	s.r.Nil(err)
	result, err := scanValues(rows)
	s.r.Nil(err)
	s.r.Equal([][]any{{"1", nil, nil, nil, nil, "NULL", "NULL", nil, nil}}, result)
	s.r.NoError(rows.Close())

	ctx := context.TODO()
	conn, err := s.db.Conn(ctx)
	s.r.Nil(err)

	_, err = conn.ExecContext(ctx, "SET format_null_as_str=0")
	s.r.Nil(err)

	rows, err = conn.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", s.table))
	s.r.Nil(err)
	result, err = scanValues(rows)
	s.r.Nil(err)
	s.r.Equal([][]any{{"1", nil, nil, nil, nil, nil, nil, nil, nil}}, result)
	s.r.NoError(rows.Close())
}

func (s *DatabendTestSuite) TestQueryNullAsStr() {
	row := s.db.QueryRow("SELECT NULL")
	var val sql.NullString
	err := row.Scan(&val)
	s.r.Nil(err)
	s.r.True(val.Valid)
	s.r.Equal("NULL", val.String)
}

func (s *DatabendTestSuite) TestQueryNull() {
	ctx := context.TODO()
	conn, err := s.db.Conn(ctx)
	s.r.Nil(err)

	_, err = conn.ExecContext(ctx, "SET format_null_as_str=0")
	s.r.Nil(err)

	row := conn.QueryRowContext(ctx, "SELECT NULL")
	var val sql.NullString
	err = row.Scan(&val)
	s.r.Nil(err)
	s.r.False(val.Valid)
	s.r.Equal("", val.String)
}
