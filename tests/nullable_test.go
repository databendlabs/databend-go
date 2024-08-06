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

	tx, err := s.db.Begin()
	s.r.Nil(err)
	_, err = s.db.Exec("SET format_null_as_str=0")
	s.r.Nil(err)
	rows, err = tx.Query(fmt.Sprintf("SELECT * FROM %s", s.table))
	s.r.Nil(err)
	result, err = scanValues(rows)
	s.r.Nil(err)
	s.r.Equal([][]interface{}{{int64(1), nil, nil, nil, nil, nil, nil, nil, nil}}, result)
	s.r.NoError(rows.Close())
	err = tx.Rollback()
	s.r.Nil(err)
}
