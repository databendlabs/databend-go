package tests

import "fmt"

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

func (s *DatabendTestSuite) TestQueryNull() {
	rows, err := s.db.Query("SELECT NULL")
	s.r.Nil(err)
	result, err := scanValues(rows)
	s.r.Nil(err)
	s.r.Equal([][]interface{}{{"NULL"}}, result)
	s.r.NoError(rows.Close())

	_, err = s.db.Exec("SET GLOBAL format_null_as_str=0")
	s.r.Nil(err)

	rows, err = s.db.Query("SELECT NULL")
	s.r.Nil(err)
	result, err = scanValues(rows)
	s.r.Nil(err)
	s.r.Equal([][]interface{}{{nil}}, result)
	s.r.NoError(rows.Close())

	_, err = s.db.Exec("UNSET format_null_as_str")
	s.r.Nil(err)
}
