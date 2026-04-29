package tests

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
)

func (s *DatabendTestSuite) TestNullable() {
	conn := s.Conn()
	ctx := context.Background()
	_, err := conn.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (i64) VALUES (?)", s.table), int64(1))
	s.r.NoError(err)

	conn = s.Conn()
	defer func() {
		s.r.NoError(conn.Close())
	}()

	_, err = conn.ExecContext(ctx, "SET format_null_as_str=0")
	s.r.NoError(err)

	rows, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", s.table))
	s.r.NoError(err)
	result, err := scanValues(rows)
	s.r.NoError(err)
	s.r.Equal([][]any{{"1", nil, nil, nil, nil, nil, nil, nil, nil}}, result)
	s.r.NoError(rows.Close())
}

func (s *DatabendTestSuite) TestQueryNull() {
	conn := s.Conn()
	defer conn.Close()

	ctx := context.Background()
	_, err := conn.ExecContext(ctx, "SET format_null_as_str=0")
	s.r.NoError(err)

	row := conn.QueryRowContext(ctx, "SELECT NULL")
	var val sql.NullString
	err = row.Scan(&val)
	s.r.NoError(err)
	s.r.False(val.Valid)
	s.r.Empty(val.String)
}

func (s *DatabendTestSuite) TestNullableExtendedTypes() {
	conn := s.Conn()
	defer conn.Close()

	ctx := context.Background()
	_, err := conn.ExecContext(ctx, "SET format_null_as_str=0")
	s.r.NoError(err)

	rows, err := conn.QueryContext(ctx, "settings(binary_output_format='base64', geometry_output_format='WKB') SELECT CAST(NULL AS Binary), CAST(NULL AS Geometry), CAST(NULL AS Geography)")
	s.r.NoError(err)

	columnTypes, err := rows.ColumnTypes()
	s.r.NoError(err)
	s.r.Len(columnTypes, 3)
	s.r.Equal("Binary NULL", columnTypes[0].DatabaseTypeName())
	s.r.Equal("Geometry NULL", columnTypes[1].DatabaseTypeName())
	s.r.Equal("Geography NULL", columnTypes[2].DatabaseTypeName())
	s.r.Equal(reflect.TypeOf([]byte(nil)), columnTypes[0].ScanType())
	s.r.Equal(reflect.TypeOf([]byte(nil)), columnTypes[1].ScanType())
	s.r.Equal(reflect.TypeOf([]byte(nil)), columnTypes[2].ScanType())

	result, err := scanValues(rows)
	s.r.NoError(err)
	s.r.Equal([][]any{{nil, nil, nil}}, result)
	s.r.NoError(rows.Close())

	row := conn.QueryRowContext(ctx, "settings(binary_output_format='base64', geometry_output_format='WKT') SELECT CAST(NULL AS Binary), CAST(NULL AS Geometry), CAST(NULL AS Geography)")
	var (
		binaryVal []byte
		geomText  sql.NullString
		geogText  sql.NullString
	)
	err = row.Scan(&binaryVal, &geomText, &geogText)
	s.r.NoError(err)
	s.r.Nil(binaryVal)
	s.r.False(geomText.Valid)
	s.r.False(geogText.Valid)
}
