package godatabend

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
)

type nextRows struct {
	dc       *DatabendConn
	ctx      context.Context
	respData *QueryResponse
	columns  []string
	types    []string
	parsers  []DataParser
}

func waitForQueryResult(ctx context.Context, dc *DatabendConn, result *QueryResponse) (*QueryResponse, error) {
	if result.Error != nil {
		return nil, result.Error
	}
	// save schema to use in the final response
	schema := result.Schema
	var err error
	for result.NextURI != "" && len(result.Data) == 0 {
		dc.log("wait for query result", result.NextURI)
		result, err = dc.rest.QueryPage(ctx, result.NextURI)
		if errors.Is(err, context.Canceled) {
			// context might be canceled due to timeout or canceled. if it's canceled, we need call
			// the kill url to tell the backend it's killed.
			dc.log("query canceled", result.ID)
			dc.rest.KillQuery(context.Background(), result.KillURI)
			return nil, err
		} else if err != nil {
			return nil, err
		}
		if result.Error != nil {
			return nil, result.Error
		}
	}
	result.Schema = schema
	return result, nil
}

func newNextRows(ctx context.Context, dc *DatabendConn, resp *QueryResponse) (*nextRows, error) {
	var columns []string
	var types []string

	result, err := waitForQueryResult(ctx, dc, resp)
	if err != nil {
		return nil, err
	}

	for _, field := range result.Schema {
		columns = append(columns, field.Name)
		types = append(types, field.Type)
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
		ctx:      ctx,
		respData: result,
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
	if len(r.respData.NextURI) != 0 {
		_, err := r.dc.rest.QueryPage(r.dc.ctx, r.respData.NextURI)
		if err != nil {
			return err
		}
	}
	r.dc.cancel = nil
	return nil
}

func (r *nextRows) Next(dest []driver.Value) error {
	if len(r.respData.Data) == 0 {
		resp, err := waitForQueryResult(r.ctx, r.dc, r.respData)
		if err != nil {
			return err
		}
		r.respData = resp
	}

	if len(r.respData.Data) == 0 {
		return io.EOF
	}

	lineData := r.respData.Data[0]
	r.respData.Data = r.respData.Data[1:]

	for j := range lineData {
		reader := strings.NewReader(lineData[j])
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
