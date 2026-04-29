package tests

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
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
	roleName := strings.ToLower(fmt.Sprintf("test_role_%d", time.Now().UnixNano()))
	defer func() {
		_, err := db.Exec(fmt.Sprintf("drop role if exists %s", roleName))
		r.NoError(err)
	}()

	err := db.QueryRow("select version()").Scan(&result)
	r.NoError(err)
	_, err = db.Exec(fmt.Sprintf("drop role if exists %s", roleName))
	r.NoError(err)
	_, err = db.Exec(fmt.Sprintf("create role if not exists %s", roleName))
	r.NoError(err)
	s.NoError(err)

	// wait for RoleCacheManager to reload
	time.Sleep(15 * time.Second)

	// message: Cannot grant role to built-in user `databend`
	//var user string
	//err = db.QueryRow("select current_user()").Scan(&user)
	//r.NoError(err)
	//_, err = db.Exec("grant role 'test_role' to " + user)
	//r.NoError(err)

	_, err = db.Exec(fmt.Sprintf("set role '%s'", roleName))
	r.NoError(err)
	err = db.QueryRow("select current_role()").Scan(&result)
	r.NoError(err)
	r.Equal(roleName, result)

	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	dsn_with_role := fmt.Sprintf("%s%srole=%s", dsn, separator, roleName)
	db2, err := sql.Open("databend", dsn_with_role)
	r.NoError(err)
	defer db2.Close()
	err = db2.QueryRow("select current_role()").Scan(&result)
	r.NoError(err)
	r.Equal(roleName, result)
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
