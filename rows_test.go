package godatabend

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTextRows(t *testing.T) {
	ptr1 := strPtr("1")
	ptr2 := strPtr("2")
	ptr3 := strPtr("2")
	rows, err := newNextRows(context.Background(), &DatabendConn{}, &QueryResponse{
		Data: [][]*string{{ptr1, ptr2, ptr3}, {ptr3, ptr2, ptr1}},
		Schema: &[]DataField{
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

func strPtr(s string) *string {
	return &s
}
