package tests

import "github.com/stretchr/testify/require"

func (s *DatabendTestSuite) TestChangeDatabase() {
	r := require.New(s.T())
	var result string

	_, err := s.db.Exec("use system")
	r.Nil(err)
	err = s.db.QueryRow("select currentDatabase()").Scan(&result)
	r.Nil(err)
	r.Equal("system", result)

	_, err = s.db.Exec("use default")
	r.Nil(err)
	err = s.db.QueryRow("select currentDatabase()").Scan(&result)
	r.Nil(err)
	r.Equal("default", result)
}

func (s *DatabendTestSuite) TestSessionConfig() {
	r := require.New(s.T())

	var result int64

	err := s.db.QueryRow("select value from system.settings where name=?", "max_result_rows").Scan(&result)
	r.Nil(err)
	r.Equal(int64(0), result)

	_, err = s.db.Exec("set max_result_rows = 100")
	r.Nil(err)
	err = s.db.QueryRow("select value from system.settings where name=?", "max_result_rows").Scan(&result)
	r.Nil(err)
	r.Equal(int64(100), result)

	_, err = s.db.Exec("unset max_result_rows")
	r.Nil(err)
	err = s.db.QueryRow("select value from system.settings where name=?", "max_result_rows").Scan(&result)
	r.Nil(err)
	r.Equal(int64(0), result)
}
