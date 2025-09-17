package tests

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
)

func (s *DatabendTestSuite) TestChangeDatabase() {
	s.SetupSuite()
	r := require.New(s.T())

	db := sql.OpenDB(s.cfg)
	defer db.Close()
	var result string

	_, err := db.Exec("use system")
	r.NoError(err)
	err = db.QueryRow("select currentDatabase()").Scan(&result)
	r.NoError(err)
	r.Equal("system", result)

	_, err = db.Exec("use default")
	r.NoError(err)
	err = db.QueryRow("select currentDatabase()").Scan(&result)
	r.NoError(err)
	r.Equal("default", result)
}

func (s *DatabendTestSuite) TestChangeRole() {
	r := require.New(s.T())
	var result string
	db := sql.OpenDB(s.cfg)
	defer db.Close()

	err := db.QueryRow("select version()").Scan(&result)
	r.NoError(err)
	_, err = db.Exec("drop role if exists test_role")
	_, err = db.Exec("drop role if exists test_role_2")
	println(result)
	_, err = db.Exec("create role if not exists test_role")
	r.NoError(err)
	s.NoError(err)

	// wait for RoleCacheManager to reload
	time.Sleep(15 * time.Second)

	_, err = db.Exec("set role 'test_role'")
	r.NoError(err)
	err = db.QueryRow("select current_role()").Scan(&result)
	r.NoError(err)
	r.Equal("test_role", result)

	dsn_with_role := fmt.Sprintf("%s&role=test_role", dsn)
	db2, err := sql.Open("databend", dsn_with_role)
	err = db2.QueryRow("select current_role()").Scan(&result)
	r.NoError(err)
	r.Equal("test_role", result)
}

func (s *DatabendTestSuite) TestSessionConfig() {
	r := require.New(s.T())
	db := sql.OpenDB(s.cfg)
	defer db.Close()

	var result int64

	err := db.QueryRow("select value from system.settings where name=?", "max_result_rows").Scan(&result)
	r.NoError(err)
	r.Equal(int64(0), result)

	_, err = db.Exec("set max_result_rows = 100")
	r.NoError(err)
	err = db.QueryRow("select value from system.settings where name=?", "max_result_rows").Scan(&result)
	r.NoError(err)
	r.Equal(int64(100), result)

	_, err = db.Exec("unset max_result_rows")
	r.NoError(err)
	err = db.QueryRow("select value from system.settings where name=?", "max_result_rows").Scan(&result)
	r.NoError(err)
	r.Equal(int64(0), result)
}

func (s *DatabendTestSuite) TestSessionVariable() {
	r := require.New(s.T())
	db := sql.OpenDB(s.cfg)
	defer db.Close()

	var result int64

	_, err := db.Exec("set variable a = 100")
	r.NoError(err)
	err = db.QueryRow("select $a").Scan(&result)
	r.NoError(err)
	r.Equal(int64(100), result)
}

func (s *DatabendTestSuite) TestTempTable() {
	r := require.New(s.T())
	db := sql.OpenDB(s.cfg)
	defer db.Close()

	var result int64
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	defer func() {
		err = conn.Close()
		r.NoError(err)
	}()
	_, err = conn.ExecContext(ctx, "create temp table t_temp (a int64)")
	r.NoError(err)
	_, err = conn.ExecContext(ctx, "insert into t_temp values (1), (2)")
	r.NoError(err)
	rows, err := conn.QueryContext(ctx, "select * from t_temp")
	r.NoError(err)
	defer rows.Close()

	r.True(rows.Next())
	err = rows.Scan(&result)
	r.Equal(int64(1), result)

	r.True(rows.Next())
	err = rows.Scan(&result)
	r.Equal(int64(2), result)

	r.False(rows.Next())
}
