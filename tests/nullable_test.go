package tests

import (
	"context"
	"database/sql"
	"fmt"
)

func (s *DatabendTestSuite) TestNullable() {
	conn := s.Conn()
	ctx := context.Background()
	_, err := conn.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (i64) VALUES (?)", s.table), int64(1))
	s.r.NoError(err)

	rows, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", s.table))
	s.r.NoError(err)
	result, err := scanValues(rows)
	s.r.NoError(err)
	s.r.Equal([][]any{{"1", nil, nil, "NULL", "NULL", "NULL", "NULL", nil, nil}}, result)
	s.r.NoError(rows.Close())
	s.r.NoError(conn.Close())

	cfg := *s.cfg
	cfg.DataParserOptions.DisableFormatNullAsStr = true
	db := sql.OpenDB(&cfg)
	conn, err = db.Conn(ctx)
	s.r.NoError(err)

	_, err = conn.ExecContext(ctx, "SET format_null_as_str=0")
	s.r.NoError(err)

	rows, err = conn.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", s.table))
	s.r.NoError(err)
	result, err = scanValues(rows)
	s.r.NoError(err)
	s.r.Equal([][]any{{"1", nil, nil, nil, nil, nil, nil, nil, nil}}, result)
	s.r.NoError(rows.Close())
}

func (s *DatabendTestSuite) TestQueryNullAsStr() {
	conn := s.Conn()
	defer conn.Close()
	row := conn.QueryRowContext(context.Background(), "SELECT NULL")
	var val sql.NullString
	err := row.Scan(&val)
	s.r.NoError(err)
	s.r.True(val.Valid)
	s.r.Equal("NULL", val.String)
}

func (s *DatabendTestSuite) TestQueryNull() {
	cfg := *s.cfg
	cfg.DataParserOptions.DisableFormatNullAsStr = true
	db := sql.OpenDB(&cfg)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	s.r.NoError(err)
	defer conn.Close()

	_, err = conn.ExecContext(ctx, "SET format_null_as_str=0")
	s.r.NoError(err)

	row := conn.QueryRowContext(ctx, "SELECT NULL")
	var val sql.NullString
	err = row.Scan(&val)
	s.r.NoError(err)
	s.r.False(val.Valid)
	s.r.Equal("", val.String)
}
