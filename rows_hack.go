//go:build rows_hack

package godatabend

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"unsafe"
)

func LastRawRow(rows *sql.Rows) []*string {
	field, ok := reflect.ValueOf((*sql.Rows)(nil)).Type().Elem().FieldByName("rowsi")
	if !ok {
		panic("rowsi field not found")
	}
	rowsi := *(*driver.Rows)(unsafe.Pointer(uintptr(unsafe.Pointer(rows)) + field.Offset))
	return rowsi.(*nextRows).latestRow
}
