package godatabend

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTextRows(t *testing.T) {
	rows, err := newNextRows(&DatabendConn{}, &QueryResponse{
		Data: [][]string{{"1", "2", "3"}, {"3", "2", "1"}},
		Schema: []DataField{
			{Name: "age", Type: "Int32"},
			{Name: "height", Type: "Int64"},
			{Name: "score", Type: "String"},
		},
		State: "Succeeded",
	})
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []string{"Int32", "Int64", "String"}, rows.types)
	assert.Equal(t, []string{"age", "height", "score"}, rows.Columns())
	assert.Equal(t, reflect.TypeOf(int32(0)), rows.ColumnTypeScanType(0))
	assert.Equal(t, reflect.TypeOf(""), rows.ColumnTypeScanType(2))
	assert.Equal(t, "Int32", rows.ColumnTypeDatabaseTypeName(0))
	assert.Equal(t, "String", rows.ColumnTypeDatabaseTypeName(2))
}
