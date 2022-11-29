package tests

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/test-go/testify/suite"

	dc "github.com/databendcloud/databend-go"
)

const (
	createTable = `CREATE TABLE %s (title VARCHAR, author VARCHAR, date VARCHAR)`
)

func TestDatabendSuite(t *testing.T) {
	suite.Run(t, new(DatabendTestSuite))
}

type DatabendTestSuite struct {
	suite.Suite
	db    *sql.DB
	table string
	r     *require.Assertions
}

func (s *DatabendTestSuite) SetupSuite() {
	var err error

	dsn := os.Getenv("TEST_DATABEND_DSN")
	s.NotEmpty(dsn)

	s.db, err = sql.Open("databend", dsn)
	s.Nil(err)

	err = s.db.Ping()
	s.Nil(err)

	var version string
	err = s.db.QueryRow("select version()").Scan(&version)
	s.Nil(err)

	s.T().Logf("connected to databend: %s\n", version)
}

func (s *DatabendTestSuite) TearDownSuite() {
	s.db.Close()
}

func (s *DatabendTestSuite) SetupTest() {
	t := s.T()
	s.r = require.New(t)

	s.table = fmt.Sprintf("test_%s_%d", t.Name(), time.Now().Unix())
	// t.Logf("setup test with table %s", s.table)

	_, err := s.db.Exec(fmt.Sprintf(createTable, s.table))
	s.r.Nil(err)
}

func (s *DatabendTestSuite) TearDownTest() {
	// t := s.T()

	// t.Logf("teardown test with table %s", s.table)
	_, err := s.db.Exec(fmt.Sprintf("DROP TABLE %s", s.table))
	s.r.Nil(err)
}

func (s *DatabendTestSuite) TestDesc() {
	rows, err := s.db.Query("DESC " + s.table)
	s.r.Nil(err)

	result, err := scanValues(rows)
	s.r.Nil(err)
	s.r.Equal([][]interface{}{
		{"title", "VARCHAR", "NO", "", ""},
		{"author", "VARCHAR", "NO", "", ""},
		{"date", "VARCHAR", "NO", "", ""},
	}, result)
	rows.Close()
}

func (s *DatabendTestSuite) TestBasicSelect() {
	query := "SELECT ?"
	result, err := s.db.Exec(query, []interface{}{1}...)
	s.r.Nil(err)

	affected, err := result.RowsAffected()
	s.r.ErrorIs(err, dc.ErrNoRowsAffected)
	s.r.Equal(int64(0), affected)
}

func (s *DatabendTestSuite) TestBatchInsert() {
	r := require.New(s.T())

	scope, err := s.db.Begin()
	r.Nil(err)

	batch, err := scope.Prepare(fmt.Sprintf("INSERT INTO %s", s.table))
	r.Nil(err)

	for i := 0; i < 10; i++ {
		_, err = batch.Exec(
			"book",
			"author",
			"2022",
		)
		r.Nil(err)
	}

	err = scope.Commit()
	r.Nil(err)
}

func scanValues(rows *sql.Rows) (interface{}, error) {
	var err error
	var result [][]interface{}
	ct, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	types := make([]reflect.Type, len(ct))
	for i, v := range ct {
		types[i] = v.ScanType()
	}
	ptrs := make([]interface{}, len(types))
	for rows.Next() {
		if err = rows.Err(); err != nil {
			return nil, err
		}
		for i, t := range types {
			ptrs[i] = reflect.New(t).Interface()
		}
		err = rows.Scan(ptrs...)
		if err != nil {
			return nil, err
		}
		values := make([]interface{}, len(types))
		for i, p := range ptrs {
			values[i] = reflect.ValueOf(p).Elem().Interface()
		}
		result = append(result, values)
	}
	return result, nil
}
