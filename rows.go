package godatabend

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
)

type nextRows struct {
	dc       *DatabendConn
	respData *QueryResponse
	columns  []string
	types    []string
	parsers  []DataParser
}

func newNextRows(dc *DatabendConn, respData *QueryResponse) (*nextRows, error) {
	var columns []string
	var types []string
	for _, field := range respData.Schema.Fields {
		columns = append(columns, field.Name)
		x := &TypeDetail{}
		res, err := json.Marshal(field.DataType)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(res, &x)
		if err != nil {
			return nil, err
		}
		types = append(types, x.Type)
	}

	parsers := make([]DataParser, len(types))
	for i, typ := range types {
		desc, err := ParseTypeDesc(typ)
		if err != nil {
			return nil, fmt.Errorf("newTextRows: failed to parse a description of the type '%s': %w", typ, err)
		}

		parsers[i], err = NewDataParser(desc, &DataParserOptions{})
		if err != nil {
			return nil, fmt.Errorf("newTextRows: failed to create a data parser for the type '%s': %w", typ, err)
		}
	}

	rows := &nextRows{
		dc:       dc,
		respData: respData,
		columns:  columns,
		types:    types,
		parsers:  parsers,
	}
	return rows, nil
}

func (r *nextRows) Columns() []string {
	return r.columns
}

func (r *nextRows) Close() error {
	// FIXME: should check & call final here
	r.dc.cancel = nil
	return nil
}

func (r *nextRows) Next(dest []driver.Value) error {
	if r.respData.State != "Succeeded" {
		return fmt.Errorf("query state: %s", r.respData.State)
	}
	r.dc.log("the state is ", r.respData.State)

	if len(r.respData.Data) == 0 {
		if r.respData.NextURI != "" {
			res, err := r.dc.rest.QueryPage(r.respData.Id, r.respData.NextURI)
			if err != nil {
				return err
			}
			r.dc.log(res.NextURI)
			r.respData = res
			if res.Error != nil {
				return err
			}
		}
	}
	if len(r.respData.Data) == 0 {
		return io.EOF
	}

	lineData := r.respData.Data[0]
	r.respData.Data = r.respData.Data[1:]

	for j := range lineData {
		reader := strings.NewReader(fmt.Sprintf("%v", lineData[j]))
		v, err := r.parsers[j].Parse(reader)
		if err != nil {
			r.dc.log("parse error ", err)
			return err
		}
		dest[j] = v
	}
	return nil
}

// ColumnTypeScanType implements the driver.RowsColumnTypeScanType
func (r *nextRows) ColumnTypeScanType(index int) reflect.Type {
	return r.parsers[index].Type()
}

// ColumnTypeDatabaseTypeName implements the driver.RowsColumnTypeDatabaseTypeName
func (r *nextRows) ColumnTypeDatabaseTypeName(index int) string {
	return r.types[index]
}

// // ColumnTypeDatabaseTypeName implements the driver.RowsColumnTypeLength
// func (r *nextRows) ColumnTypeLength(index int) (int64, bool) {
// 	// TODO: implement this
// 	return 10, true
// }

// // ColumnTypeDatabaseTypeName implements the driver.RowsColumnTypeNullable
// func (r *nextRows) ColumnTypeNullable(index int) (bool, bool) {
// 	// TODO: implement this
// 	return true, true
// }

// // ColumnTypeDatabaseTypeName implements the driver.RowsColumnTypePrecisionScale
// func (r *nextRows) ColumnTypePrecisionScale(index int) (int64, int64, bool) {
// 	// TODO: implement this
// 	return 10, 10, true
// }
