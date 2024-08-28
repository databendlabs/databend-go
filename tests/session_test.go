package tests

import (
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/require"
)

func (s *DatabendTestSuite) TestChangeDatabase() {
	s.SetupSuite()
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

func (s *DatabendTestSuite) TestChangeRole() {
	r := require.New(s.T())
	var result string
	err := s.db.QueryRow("select version()").Scan(&result)
	r.Nil(err)
	println(result)
	_, err = s.db.Exec("create role if not exists test_role")
	r.Nil(err)

	s.NotEmpty(dsn)
	dsn_with_role := fmt.Sprintf("%s&role=test_role", dsn)
	s.db, err = sql.Open("databend", dsn_with_role)
	s.Nil(err)

	err = s.db.QueryRow("select current_role()").Scan(&result)
	r.Nil(err)
	r.Equal("test_role", result)

	s.NotEmpty(dsn)
	s.db, err = sql.Open("databend", dsn)
	s.Nil(err)
	//
	//defer s.db.Exec("drop role if exists test_role")
	//_, err = s.db.Exec("set role 'test_role'")
	//r.Nil(err)
	//

	_, err = s.db.Exec("create role if not exists test_role_2")
	r.Nil(err)
	//defer s.db.Exec("drop role if exists test_role_2")
	_, err = s.db.Exec("set role 'test_role_2'")
	r.Nil(err)
	err = s.db.QueryRow("select current_role()").Scan(&result)
	r.Nil(err)
	// skip now
	//r.Equal("test_role_2", result)
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

func (s *DatabendTestSuite) TestSessionVariable() {
	r := require.New(s.T())

	var result int64

	_, err := s.db.Exec("set variable a = 100")
	r.Nil(err)
	err = s.db.QueryRow("select $a").Scan(&result)
	r.Nil(err)
	r.Equal(int64(100), result)
}
