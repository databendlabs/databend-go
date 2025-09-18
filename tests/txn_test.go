package tests

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTnx(t *testing.T) {
	selectT := "SELECT * FROM t ORDER BY c;"
	db1, err := sql.Open("databend", dsn)
	assert.NoError(t, err)
	db2, err := sql.Open("databend", dsn)
	assert.NoError(t, err)

	// test commit
	_, err = db1.Exec("CREATE OR REPLACE TABLE t(c int);")
	assert.NoError(t, err)
	tx1, err := db1.Begin()
	assert.NoError(t, err)
	_, err = tx1.Exec("INSERT INTO t(c) VALUES(1);")
	assert.NoError(t, err)
	rows, err := tx1.Query("select 1")
	assert.NoError(t, err)
	assert.True(t, rows.Next())
	rows.Close()

	rows2, err := db2.Query(selectT)
	assert.NoError(t, err)
	assert.False(t, rows2.Next())

	tx2, err := db2.Begin()
	assert.NoError(t, err)

	_, err = tx2.Exec("INSERT INTO t(c) VALUES(2);")
	assert.NoError(t, err)
	rows2, err = tx2.Query(selectT)
	assert.NoError(t, err)
	assert.True(t, rows2.Next())

	rows1, err := tx1.Query("select 2")
	assert.NoError(t, err)
	assert.True(t, rows1.Next())
	rows1.Close()

	err = tx2.Commit()
	assert.NoError(t, err)
	err = tx1.Commit()
	assert.NoError(t, err)

	rows1, err = db1.Query(selectT)
	assert.NoError(t, err)
	if rows1 != nil {
		res1, _ := scanValues(rows1)
		assert.Equal(t, [][]any{{"1"}, {"2"}}, res1)
	}
	rows2, err = db2.Query(selectT)
	assert.NoError(t, err)
	if rows2 != nil {
		res2, _ := scanValues(rows2)
		assert.Equal(t, [][]any{{"1"}, {"2"}}, res2)
	}

	// test rollback
	_, err = db1.Exec("DROP table  t;")
	assert.NoError(t, err)
	_, err = db1.Exec("CREATE OR REPLACE TABLE t(c int);")
	assert.NoError(t, err)
	tx1, err = db1.Begin()
	assert.NoError(t, err)
	_, err = tx1.Exec("INSERT INTO t(c) VALUES(1);")
	assert.NoError(t, err)
	rows, err = tx1.Query(selectT)
	assert.NoError(t, err)
	assert.True(t, rows.Next())
	rows.Close()
	tx1.Rollback()
	rows1, err = db1.Query(selectT)
	assert.NoError(t, err)
	if rows1 != nil {
		assert.False(t, rows1.Next())
	}

}
