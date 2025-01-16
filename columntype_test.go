package godatabend

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestColumnType(t *testing.T) {
	tests := []struct {
		typeDesc string
		input    string
		want     any
	}{
		{typeDesc: "String", input: "123", want: "123"},
		{typeDesc: "Nullable(String)", input: "123", want: "123"},
		{typeDesc: "Boolean", input: "1", want: true},
		{typeDesc: "Int8", input: "123", want: int8(123)},
		{typeDesc: "Int16", input: "123", want: int16(123)},
		{typeDesc: "Int32", input: "123", want: int32(123)},
		{typeDesc: "Int64", input: "123", want: int64(123)},
		{typeDesc: "UInt8", input: "123", want: uint8(123)},
		{typeDesc: "UInt16", input: "123", want: uint16(123)},
		{typeDesc: "UInt32", input: "123", want: uint32(123)},
		{typeDesc: "UInt64", input: "123", want: uint64(123)},
		{typeDesc: "Float32", input: "123.0", want: float32(123)},
		{typeDesc: "Float64", input: "123.0", want: float64(123)},
		{typeDesc: "Timestamp", input: "2025-01-16 02:01:26.739219", want: time.Date(2025, 1, 16, 2, 1, 26, 739219000, time.UTC)},
		{typeDesc: "Date", input: "2025-01-16", want: time.Date(2025, 1, 16, 0, 0, 0, 0, time.UTC)},
		{typeDesc: "Decimal(10, 2)", input: "123.45", want: "123.45"},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%s::%s", tc.input, tc.typeDesc), func(t *testing.T) {
			colType, err := NewColumnType(tc.typeDesc, nil)
			require.NoError(t, err)

			v, err := colType.Parse(tc.input)
			require.NoError(t, err)
			require.True(t, driver.IsValue(v))

			require.Equal(t, reflect.TypeOf(tc.want).Name(), colType.ScanType().Name())

			desc, err := ParseTypeDesc(tc.typeDesc)
			require.NoError(t, err)
			desc = desc.Normalize()

			desc2, err := ParseTypeDesc(colType.DatabaseTypeName())
			require.NoError(t, err)
			require.Equal(t, desc, desc2)

			runScan(t, tc.typeDesc, tc.input, tc.want)
		})
	}
}

func runScan(t *testing.T, desc string, input string, want any) {
	db := sql.OpenDB(&fakeConnector{
		resp: &QueryResponse{
			Schema: &[]DataField{{Name: "x", Type: desc}},
			Data:   [][]*string{{&input}},
		},
	})

	rows, err := db.Query("x")
	require.NoError(t, err)

	rows.Next()

	types, err := rows.ColumnTypes()
	require.NoError(t, err)

	a := reflect.New(types[0].ScanType()).Interface()
	rows.Scan(a)
	require.Equal(t, want, reflect.ValueOf(a).Elem().Interface())
}

type fakeConnector struct {
	resp *QueryResponse
}

func (c *fakeConnector) Driver() driver.Driver {
	return nil
}

func (c *fakeConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return &fakeConn{c.resp}, nil
}

type fakeConn struct {
	resp *QueryResponse
}

func (c *fakeConn) Prepare(query string) (driver.Stmt, error) {
	return &fakeStmt{
		resp: c.resp,
	}, nil
}

func (c *fakeConn) Close() error {
	return nil
}

func (c *fakeConn) Begin() (driver.Tx, error) {
	return nil, nil
}

type fakeStmt struct {
	resp *QueryResponse
}

func (s *fakeStmt) Close() error {
	return nil
}

func (s *fakeStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, nil
}

func (s *fakeStmt) NumInput() int {
	return 0
}

func (s *fakeStmt) Query(args []driver.Value) (driver.Rows, error) {
	schema, err := parse_schema(s.resp.Schema, nil)
	if err != nil {
		return nil, err
	}

	return &nextRows{
		dc:           &DatabendConn{},
		respData:     s.resp,
		resultSchema: *schema,
	}, nil
}
