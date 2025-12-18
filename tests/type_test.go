package tests

import (
	"database/sql"
	"fmt"
	databend "github.com/datafuselabs/databend-go"
	"golang.org/x/mod/semver"
	"time"
)

func (s *DatabendTestSuite) TestDate() {
	if semver.Compare(driverVersion, "v0.9.0") <= 0 && semver.Compare(serverVersion, "1.2.836") < 0 {
		return
	}

	today := time.Date(2025, 1, 16, 0, 0, 0, 0, time.UTC)
	lastday := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)

	type testCase struct {
		name string
		// scan get Date with location in settings
		setting *time.Location
		data    *time.Location
		exp     time.Time
	}

	locShanghai, _ := time.LoadLocation("Asia/Shanghai")
	locLos, _ := time.LoadLocation("America/Los_Angeles")
	testCases := []testCase{
		{name: "1", setting: time.UTC, data: time.UTC, exp: today},
		{name: "2", setting: locShanghai, data: locShanghai, exp: today},
		{name: "3", setting: time.UTC, data: locShanghai, exp: today},
		{name: "4", setting: locShanghai, data: time.UTC, exp: today},
		{name: "5", setting: time.UTC, data: locLos, exp: lastday},
	}

	for i, tc := range testCases {
		s.Run(tc.name, func() {
			db := sql.OpenDB(s.cfg)
			defer db.Close()

			tableName := fmt.Sprintf("test_%s_%d", s.table, i)
			result, err := db.Exec(fmt.Sprintf("create or replace table %s (d Date)", tableName))
			s.r.NoError(err)

			result, err = db.Exec(fmt.Sprintf("set timezone='%v'", tc.setting.String()))
			s.r.NoError(err)

			insertSQL := fmt.Sprintf("INSERT INTO %s VALUES (?)", tableName)

			// not have to use databend.Date
			result, err = db.Exec(insertSQL, today.In(tc.data))
			s.r.NoError(err)

			result, err = db.Exec(insertSQL, databend.Date(today.In(tc.data)))
			s.r.NoError(err)
			n, err := result.RowsAffected()
			s.r.NoError(err)
			s.r.Equal(int64(1), n)

			selectSQL := fmt.Sprintf("select * from %s", tableName)
			rows, err := db.Query(selectSQL)
			s.r.NoError(err)

			var output time.Time
			for i := 0; i < 2; i++ {
				s.r.True(rows.Next())
				s.r.NoError(err)
				err = rows.Scan(&output)
				s.r.NoError(err)
				// always return Time in UTC
				s.r.Equal(tc.exp, output)
			}

			s.r.NoError(rows.Close())
		})
	}
}

func (s *DatabendTestSuite) TestTimestamp() {
	if semver.Compare(driverVersion, "v0.9.0") <= 0 && semver.Compare(serverVersion, "1.2.836") < 0 {
		return
	}

	input := time.Date(2025, 1, 16, 2, 1, 26, 739219000, time.UTC)

	type testCase struct {
		name string
		// scan should get Time with location in settings
		setting *time.Location
		data    *time.Location
	}

	locShanghai, _ := time.LoadLocation("Asia/Shanghai")
	testCases := []testCase{
		{name: "1", setting: time.UTC, data: time.UTC},
		{name: "2", setting: locShanghai, data: locShanghai},
		{name: "3", setting: time.UTC, data: locShanghai},
		{name: "4", setting: locShanghai, data: time.UTC},
	}

	for i, tc := range testCases {
		s.Run(tc.name, func() {
			db := sql.OpenDB(s.cfg)
			defer db.Close()

			input = input.In(tc.data)

			tableName := fmt.Sprintf("test_%s_%d", s.table, i)
			result, err := db.Exec(fmt.Sprintf("create or replace table %s (t DateTime)", tableName))
			s.r.NoError(err)

			result, err = db.Exec(fmt.Sprintf("set timezone='%v'", tc.setting.String()))
			s.r.NoError(err)

			insertSQL := fmt.Sprintf("INSERT INTO %s VALUES (?)", tableName)
			result, err = db.Exec(insertSQL, input)
			s.r.NoError(err)
			n, err := result.RowsAffected()
			s.r.NoError(err)
			s.r.Equal(int64(1), n)

			selectSQL := fmt.Sprintf("select * from %s", tableName)
			rows, err := db.Query(selectSQL)
			s.r.NoError(err)
			s.r.True(rows.Next())
			s.r.NoError(err)

			var output time.Time
			err = rows.Scan(&output)
			s.r.NoError(err)

			exp := input.In(tc.setting)
			s.r.Equal(exp, output)

			s.r.NoError(rows.Close())
		})
	}
}
