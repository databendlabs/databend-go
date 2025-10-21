package godatabend

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync/atomic"
)

var rowsHack = false

type resultSchema struct {
	columns []string
	types   []ColumnType
}

type nextRows struct {
	resultSchema
	isClosed  int32
	dc        *DatabendConn
	ctx       context.Context
	respData  *QueryResponse
	latestRow []*string
}

func waitForData(ctx context.Context, dc *DatabendConn, response *QueryResponse) (*QueryResponse, error) {
	if response.Error != nil {
		return nil, response.Error
	}
	for !response.ReadFinished() && len(response.Data) == 0 && response.Error == nil {
		nextResponse, err := dc.rest.PollQuery(ctx, response.NextURI)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				// context might be canceled due to timeout or canceled. if it's canceled, we need call
				// the kill url to tell the backend it's killed.
				dc.log("query canceled", response.ID)
				_ = dc.rest.KillQuery(context.Background(), response)
			} else {
				_ = dc.rest.CloseQuery(ctx, response)
			}
			return nil, err
		}
		response = nextResponse
		if response.Error != nil {
			_ = dc.rest.CloseQuery(ctx, response)
			return nil, fmt.Errorf("query error: %+v", response.Error)
		}
	}
	return response, nil
}

func parse_schema(fields *[]DataField, opts *ColumnTypeOptions) (*resultSchema, error) {
	if fields == nil {
		return &resultSchema{}, nil
	}

	schema := &resultSchema{
		columns: make([]string, 0, len(*fields)),
		types:   make([]ColumnType, 0, len(*fields)),
	}

	for _, field := range *fields {
		schema.columns = append(schema.columns, field.Name)
		parser, err := NewColumnType(field.Type, opts)
		if err != nil {
			return nil, fmt.Errorf("newTextRows: failed to create a data parser for the type '%s': %w", field.Type, err)
		}
		schema.types = append(schema.types, parser)
	}

	return schema, nil
}

func (dc *DatabendConn) newNextRows(ctx context.Context, resp *QueryResponse) (*nextRows, error) {
	schema, err := parse_schema(resp.Schema, dc.columnTypeOptions())
	if err != nil {
		return nil, err
	}

	rows := &nextRows{
		dc:           dc,
		ctx:          ctx,
		respData:     resp,
		resultSchema: *schema,
	}
	return rows, nil
}

func (r *nextRows) Columns() []string {
	return r.columns
}

// Close will only be called by sql.Rows once.
// But we can doClose internally as soon as EOF.
//
// Not return error for now.
//
// Note it will also be Called by framework when:
//  1. Canceling query/txn Context.
//  2. Next return error other than io.EOF.
func (r *nextRows) Close() error {
	return r.doClose()
}

func (r *nextRows) doClose() error {
	if atomic.CompareAndSwapInt32(&r.isClosed, 0, 1) {
		if r.respData != nil && len(r.respData.FinalURI) != 0 {
			err := r.dc.rest.CloseQuery(r.dc.ctx, r.respData)
			if err != nil {
				return err
			}
			r.respData = nil
		}
		r.dc.cancel = nil
		return nil
	} else {
		// Rows should be safe to close multi times
		return nil
	}
}

func (r *nextRows) Next(dest []driver.Value) error {
	if atomic.LoadInt32(&r.isClosed) == 1 || r.respData == nil {
		// If user already called Rows.Close(), Rows.Next() will not get here.
		// Get here only because we doClose() internally,
		// only when call Rows.Next() again after it return false.
		return io.EOF
	}
	if len(r.respData.Data) == 0 {
		var err error
		r.respData, err = waitForData(r.ctx, r.dc, r.respData)
		if err != nil {
			return err
		}
	}

	if len(r.respData.Data) == 0 {
		_ = r.doClose()
		return io.EOF
	}

	lineData := r.respData.Data[0]
	r.respData.Data = r.respData.Data[1:]
	if rowsHack {
		r.latestRow = lineData
	}

	for j := range dest {
		val := lineData[j]
		if val == nil {
			dest[j] = nil
			continue
		}
		v, err := r.types[j].Parse(*val)
		if err != nil {
			r.dc.log("fail to parse field", j, ", error: ", err)
			return err
		}
		dest[j] = v
	}
	return nil
}

var _ driver.RowsColumnTypeScanType = (*nextRows)(nil)

func (r *nextRows) ColumnTypeScanType(index int) reflect.Type {
	return r.types[index].ScanType()
}

var _ driver.RowsColumnTypeDatabaseTypeName = (*nextRows)(nil)

func (r *nextRows) ColumnTypeDatabaseTypeName(index int) string {
	return r.types[index].DatabaseTypeName()
}

var _ driver.RowsColumnTypeNullable = (*nextRows)(nil)

func (r *nextRows) ColumnTypeNullable(index int) (bool, bool) {
	return r.types[index].Nullable()
}

var _ driver.RowsColumnTypeLength = (*nextRows)(nil)

func (r *nextRows) ColumnTypeLength(index int) (length int64, ok bool) {
	return 0, false
}

var _ driver.RowsColumnTypePrecisionScale = (*nextRows)(nil)

func (r *nextRows) ColumnTypePrecisionScale(index int) (int64, int64, bool) {
	// TODO: implement this
	return 0, 0, false
}
