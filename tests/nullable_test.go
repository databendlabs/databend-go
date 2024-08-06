package tests

import (
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
	s.r.Equal([][]interface{}{{int64(1), nil, nil, "NULL", "NULL", nil, nil, nil, nil}}, result)
	s.r.NoError(rows.Close())

	_, err = s.db.Exec("SET GLOBAL format_null_as_str=0")
	s.r.Nil(err)

	rows, err = s.db.Query(fmt.Sprintf("SELECT * FROM %s", s.table))
	s.r.Nil(err)
	result, err = scanValues(rows)
	s.r.Nil(err)
	s.r.Equal([][]interface{}{{int64(1), nil, nil, nil, nil, nil, nil, nil, nil}}, result)
	s.r.NoError(rows.Close())

	_, err = s.db.Exec("UNSET format_null_as_str")
	s.r.Nil(err)
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
	_, err := s.db.Exec("SET GLOBAL format_null_as_str=0")
	s.r.Nil(err)

	row := s.db.QueryRow("SELECT NULL")
	var val sql.NullString
	err = row.Scan(&val)
	s.r.Nil(err)
	s.r.False(val.Valid)
	s.r.Equal("", val.String)

	_, err = s.db.Exec("UNSET format_null_as_str")
	s.r.Nil(err)
}
