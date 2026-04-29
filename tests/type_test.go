package tests

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	databend "github.com/datafuselabs/databend-go"
	"golang.org/x/mod/semver"
	"reflect"
	"time"
)

func (s *DatabendTestSuite) TestDate() {
	if semver.Compare(serverVersion, "1.2.836") < 0 {
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

			columnTypes, err := rows.ColumnTypes()
			s.r.NoError(err)
			s.r.Len(columnTypes, 1)
			s.r.Equal("Date NULL", columnTypes[0].DatabaseTypeName())
			s.r.Equal(reflect.TypeOf(time.Time{}), columnTypes[0].ScanType())

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
	if semver.Compare(serverVersion, "1.2.836") < 0 {
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

			columnTypes, err := rows.ColumnTypes()
			s.r.NoError(err)
			s.r.Len(columnTypes, 1)
			s.r.Equal("Timestamp NULL", columnTypes[0].DatabaseTypeName())
			s.r.Equal(reflect.TypeOf(time.Time{}), columnTypes[0].ScanType())

			var output time.Time
			err = rows.Scan(&output)
			s.r.NoError(err)

			exp := input.In(tc.setting)
			s.r.Equal(exp, output)

			s.r.NoError(rows.Close())
		})
	}
}

func (s *DatabendTestSuite) TestTimestampTz() {
	if semver.Compare(serverVersion, "1.2.844") < 0 {
		return
	}

	type testCase struct {
		name string
		// scan should get Time with location in settings
		setting *time.Location
		data    *time.Location
	}
	locShanghai, _ := time.LoadLocation("Asia/Shanghai")
	locLos, _ := time.LoadLocation("America/Los_Angeles")
	input := time.Date(2025, 1, 16, 2, 1, 26, 739219000, locLos)

	testCases := []testCase{
		{name: "1", setting: time.UTC},
		{name: "2", setting: locShanghai},
	}

	for i, tc := range testCases {
		s.Run(tc.name, func() {
			db := sql.OpenDB(s.cfg)
			defer db.Close()

			tableName := fmt.Sprintf("test_tz_%s_%d", s.table, i)
			result, err := db.Exec(fmt.Sprintf("create or replace table %s (t Timestamp_TZ)", tableName))
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

			columnTypes, err := rows.ColumnTypes()
			s.r.NoError(err)
			s.r.Len(columnTypes, 1)
			s.r.Equal("Timestamp_Tz NULL", columnTypes[0].DatabaseTypeName())
			s.r.Equal(reflect.TypeOf(time.Time{}), columnTypes[0].ScanType())

			var output time.Time
			err = rows.Scan(&output)
			s.r.NoError(err)

			s.r.Equal(input.UnixMicro(), output.UnixMicro())
			name, offset := output.Zone()
			s.r.Equal(-8*3600, offset)
			s.r.Equal("", name)
			s.r.NoError(rows.Close())
		})
	}
}

func (s *DatabendTestSuite) TestDecimal() {
	db := sql.OpenDB(s.cfg)
	defer db.Close()

	tableName := fmt.Sprintf("test_decimal_%s", s.table)
	_, err := db.Exec(fmt.Sprintf("create or replace table %s (d Decimal(18, 4))", tableName))
	s.r.NoError(err)
	defer func() {
		_, err := db.Exec(fmt.Sprintf("drop table %s", tableName))
		s.r.NoError(err)
	}()

	result, err := db.Exec(fmt.Sprintf("insert into %s select cast(? as Decimal(18, 4))", tableName), "12345.6789")
	s.r.NoError(err)

	n, err := result.RowsAffected()
	s.r.NoError(err)
	s.r.Equal(int64(1), n)

	rows, err := db.Query(fmt.Sprintf("select * from %s", tableName))
	s.r.NoError(err)

	columnTypes, err := rows.ColumnTypes()
	s.r.NoError(err)
	s.r.Len(columnTypes, 1)
	s.r.Equal("Decimal(18, 4) NULL", columnTypes[0].DatabaseTypeName())
	s.r.Equal(reflect.TypeOf(""), columnTypes[0].ScanType())
	nullable, ok := columnTypes[0].Nullable()
	s.r.True(ok)
	s.r.True(nullable)

	precision, scale, ok := columnTypes[0].DecimalSize()
	s.r.True(ok)
	s.r.Equal(int64(18), precision)
	s.r.Equal(int64(4), scale)

	s.r.True(rows.Next())

	var output string
	err = rows.Scan(&output)
	s.r.NoError(err)
	s.r.Equal("12345.6789", output)

	s.r.NoError(rows.Close())
}

func (s *DatabendTestSuite) TestScalarMappings() {
	db := sql.OpenDB(s.cfg)
	defer db.Close()

	rows, err := db.Query("SELECT CAST(true AS BOOLEAN) AS b, CAST(12 AS Int8) AS i8, CAST(1234 AS Int16) AS i16, CAST(123456 AS Int32) AS i32, CAST(123456789 AS Int64) AS i64, CAST(12 AS UInt8) AS u8, CAST(1234 AS UInt16) AS u16, CAST(123456 AS UInt32) AS u32, CAST(123456789 AS UInt64) AS u64, CAST(12.5 AS Float32) AS f32, CAST(34.25 AS Float64) AS f64, CAST('hello' AS String) AS s")
	s.r.NoError(err)
	s.r.True(rows.Next())

	columnTypes, err := rows.ColumnTypes()
	s.r.NoError(err)
	s.r.Len(columnTypes, 12)

	testCases := []struct {
		index    int
		dbType   string
		scanType reflect.Type
	}{
		{index: 0, dbType: "Boolean", scanType: reflect.TypeOf(true)},
		{index: 1, dbType: "Int8", scanType: reflect.TypeOf(int8(0))},
		{index: 2, dbType: "Int16", scanType: reflect.TypeOf(int16(0))},
		{index: 3, dbType: "Int32", scanType: reflect.TypeOf(int32(0))},
		{index: 4, dbType: "Int64", scanType: reflect.TypeOf(int64(0))},
		{index: 5, dbType: "UInt8", scanType: reflect.TypeOf(uint8(0))},
		{index: 6, dbType: "UInt16", scanType: reflect.TypeOf(uint16(0))},
		{index: 7, dbType: "UInt32", scanType: reflect.TypeOf(uint32(0))},
		{index: 8, dbType: "UInt64", scanType: reflect.TypeOf(uint64(0))},
		{index: 9, dbType: "Float32", scanType: reflect.TypeOf(float32(0))},
		{index: 10, dbType: "Float64", scanType: reflect.TypeOf(float64(0))},
		{index: 11, dbType: "String", scanType: reflect.TypeOf("")},
	}

	for _, tc := range testCases {
		s.r.Equal(tc.dbType, columnTypes[tc.index].DatabaseTypeName())
		s.r.Equal(tc.scanType, columnTypes[tc.index].ScanType())
		nullable, ok := columnTypes[tc.index].Nullable()
		s.r.True(ok)
		s.r.False(nullable)
	}

	var (
		b   bool
		i8  int8
		i16 int16
		i32 int32
		i64 int64
		u8  uint8
		u16 uint16
		u32 uint32
		u64 uint64
		f32 float32
		f64 float64
		str string
	)
	err = rows.Scan(&b, &i8, &i16, &i32, &i64, &u8, &u16, &u32, &u64, &f32, &f64, &str)
	s.r.NoError(err)
	s.r.True(b)
	s.r.Equal(int8(12), i8)
	s.r.Equal(int16(1234), i16)
	s.r.Equal(int32(123456), i32)
	s.r.Equal(int64(123456789), i64)
	s.r.Equal(uint8(12), u8)
	s.r.Equal(uint16(1234), u16)
	s.r.Equal(uint32(123456), u32)
	s.r.Equal(uint64(123456789), u64)
	s.r.Equal(float32(12.5), f32)
	s.r.Equal(float64(34.25), f64)
	s.r.Equal("hello", str)
	s.r.NoError(rows.Close())
}

func (s *DatabendTestSuite) TestNullableScalarMappings() {
	db := sql.OpenDB(s.cfg)
	defer db.Close()

	rows, err := db.Query("SELECT CAST(NULL AS Nullable(Boolean)) AS b, CAST(NULL AS Nullable(Int64)) AS i64, CAST(NULL AS Nullable(Float64)) AS f64, CAST(NULL AS Nullable(String)) AS s")
	s.r.NoError(err)
	s.r.True(rows.Next())

	columnTypes, err := rows.ColumnTypes()
	s.r.NoError(err)
	s.r.Len(columnTypes, 4)

	testCases := []struct {
		index    int
		dbType   string
		scanType reflect.Type
	}{
		{index: 0, dbType: "Boolean NULL", scanType: reflect.TypeOf(true)},
		{index: 1, dbType: "Int64 NULL", scanType: reflect.TypeOf(int64(0))},
		{index: 2, dbType: "Float64 NULL", scanType: reflect.TypeOf(float64(0))},
		{index: 3, dbType: "String NULL", scanType: reflect.TypeOf("")},
	}

	for _, tc := range testCases {
		s.r.Equal(tc.dbType, columnTypes[tc.index].DatabaseTypeName())
		s.r.Equal(tc.scanType, columnTypes[tc.index].ScanType())
		nullable, ok := columnTypes[tc.index].Nullable()
		s.r.True(ok)
		s.r.True(nullable)
	}

	var (
		b   sql.NullBool
		i64 sql.NullInt64
		f64 sql.NullFloat64
		str sql.NullString
	)
	err = rows.Scan(&b, &i64, &f64, &str)
	s.r.NoError(err)
	s.r.False(b.Valid)
	s.r.False(i64.Valid)
	s.r.False(f64.Valid)
	s.r.False(str.Valid)
	s.r.NoError(rows.Close())
}

func (s *DatabendTestSuite) TestGeo() {
	db := sql.OpenDB(s.cfg)
	defer db.Close()

	const (
		wkbHex       = "01010000000000000000004e400000000000804240"
		geometryWKT  = "POINT(60 37)"
		geographyWKT = "POINT(60 37)"
	)

	rows, err := db.Query("settings(geometry_output_format='WKB') SELECT to_geometry('POINT(60 37)'), CAST(NULL AS Geometry), to_geography('POINT(60 37)'), CAST(NULL AS Geography)")
	s.r.NoError(err)
	s.r.True(rows.Next())

	columnTypes, err := rows.ColumnTypes()
	s.r.NoError(err)
	s.r.Len(columnTypes, 4)
	s.r.Equal("Geometry", columnTypes[0].DatabaseTypeName())
	s.r.Equal("Geometry NULL", columnTypes[1].DatabaseTypeName())
	s.r.Equal("Geography", columnTypes[2].DatabaseTypeName())
	s.r.Equal("Geography NULL", columnTypes[3].DatabaseTypeName())
	s.r.Equal(reflect.TypeOf([]byte(nil)), columnTypes[0].ScanType())
	s.r.Equal(reflect.TypeOf([]byte(nil)), columnTypes[1].ScanType())
	s.r.Equal(reflect.TypeOf([]byte(nil)), columnTypes[2].ScanType())
	s.r.Equal(reflect.TypeOf([]byte(nil)), columnTypes[3].ScanType())

	var geomWKB, geomNull, geogWKB, geogNull []byte
	err = rows.Scan(&geomWKB, &geomNull, &geogWKB, &geogNull)
	s.r.NoError(err)
	s.r.Equal(wkbHex, hex.EncodeToString(geomWKB))
	s.r.Nil(geomNull)
	s.r.Equal(wkbHex, hex.EncodeToString(geogWKB))
	s.r.Nil(geogNull)
	s.r.NoError(rows.Close())

	rows, err = db.Query("settings(geometry_output_format='WKT') SELECT to_geometry('POINT(60 37)'), CAST(NULL AS Geometry), to_geography('POINT(60 37)'), CAST(NULL AS Geography)")
	s.r.NoError(err)
	s.r.True(rows.Next())

	columnTypes, err = rows.ColumnTypes()
	s.r.NoError(err)
	s.r.Len(columnTypes, 4)
	s.r.Equal(reflect.TypeOf(""), columnTypes[0].ScanType())
	s.r.Equal(reflect.TypeOf(""), columnTypes[1].ScanType())
	s.r.Equal(reflect.TypeOf(""), columnTypes[2].ScanType())
	s.r.Equal(reflect.TypeOf(""), columnTypes[3].ScanType())

	var geomText, geogText string
	var geomNullText, geogNullText sql.NullString
	err = rows.Scan(&geomText, &geomNullText, &geogText, &geogNullText)
	s.r.NoError(err)
	s.r.Equal(geometryWKT, geomText)
	s.r.False(geomNullText.Valid)
	s.r.Equal(geographyWKT, geogText)
	s.r.False(geogNullText.Valid)
	s.r.NoError(rows.Close())
}

func (s *DatabendTestSuite) TestBinary() {
	db := sql.OpenDB(s.cfg)
	defer db.Close()

	rows, err := db.Query("settings(binary_output_format='base64') SELECT to_binary(''), to_binary('hello'), CAST(NULL AS Binary)")
	s.r.NoError(err)
	s.r.True(rows.Next())

	columnTypes, err := rows.ColumnTypes()
	s.r.NoError(err)
	s.r.Len(columnTypes, 3)
	s.r.Equal("Binary", columnTypes[0].DatabaseTypeName())
	s.r.Equal("Binary", columnTypes[1].DatabaseTypeName())
	s.r.Equal("Binary NULL", columnTypes[2].DatabaseTypeName())
	s.r.Equal(reflect.TypeOf([]byte(nil)), columnTypes[0].ScanType())
	s.r.Equal(reflect.TypeOf([]byte(nil)), columnTypes[1].ScanType())
	s.r.Equal(reflect.TypeOf([]byte(nil)), columnTypes[2].ScanType())

	var emptyRaw, raw, nullRaw []byte
	err = rows.Scan(&emptyRaw, &raw, &nullRaw)
	s.r.NoError(err)
	s.r.Empty(emptyRaw)
	s.r.Equal([]byte("hello"), raw)
	s.r.Nil(nullRaw)
	s.r.NoError(rows.Close())

	rows, err = db.Query("settings(binary_output_format='base64') SELECT to_binary(''), to_binary('hello'), CAST(NULL AS Binary)")
	s.r.NoError(err)
	s.r.True(rows.Next())

	var emptyText, text string
	var nullText sql.NullString
	err = rows.Scan(&emptyText, &text, &nullText)
	s.r.NoError(err)
	s.r.Equal("", emptyText)
	s.r.Equal("hello", text)
	s.r.False(nullText.Valid)
	s.r.NoError(rows.Close())
}
