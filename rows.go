package godatabend

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

type nextRows struct {
	dc       *DatabendConn
	respData QueryResponse
	columns  []string
	types    []string
	parsers  []DataParser
}

func newNextRows(dc *DatabendConn, respData *QueryResponse) (*nextRows, error) {
	columns := make([]string, 0)
	types := make([]string, 0)
	for _, field := range respData.Schema.Fields {
		columns = append(columns, field.Name)
		x := &TypeDetail{}
		res, err := json.Marshal(field.DataType)
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
	return &nextRows{
		dc:       dc,
		respData: *respData,
		columns:  columns,
		types:    types,
		parsers:  parsers,
	}, nil
}

func (r *nextRows) Columns() []string {
	return r.columns
}

func (r *nextRows) Close() error {
	r.dc.cancel = nil
	return nil
}

func (r *nextRows) Next(dest []driver.Value) error {
	if len(r.respData.Data) == 0 {
		return fmt.Errorf("end")
	}
	lineData := r.respData.Data[0]
	r.respData.Data = r.respData.Data[1:]

	for j := range lineData {
		reader := strings.NewReader(fmt.Sprintf("%v", lineData[j]))
		v, err := r.parsers[j].Parse(reader)
		if err != nil {
			return err
		}
		dest[j] = v
	}
	if len(dest) != 0 {
		return nil
	}
	if r.respData.State == "Succeeded" && len(r.respData.Data) == 0 {
		r.respData = QueryResponse{}
		return nil
	}
	res, err := r.dc.rest.QueryPage(r.respData.Id, r.respData.NextURI)
	if err != nil {
		return err
	}

	r.respData = *res
	if res.Error != nil {
		return err
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
