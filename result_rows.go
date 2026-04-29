package godatabend

import (
	"database/sql/driver"
	"time"

	"github.com/pkg/errors"
)

func queryResponseColumnTypeOptions(settings *Settings) (*ColumnTypeOptions, error) {
	opts := defaultColumnTypeOptions()
	if settings == nil {
		return opts, nil
	}
	opts.SetGeometryOutputFormat(settings.GeometryOutputFormat)
	if settings.TimeZone == "" {
		return opts, nil
	}

	location, err := time.LoadLocation(settings.TimeZone)
	if err != nil {
		return nil, err
	}
	opts.SetTimezone(location)
	return opts, nil
}

func materializeJSONQueryRows(resp *QueryResponse) error {
	if resp == nil || resp.typedRows != nil || len(resp.Data) == 0 {
		return nil
	}
	if resp.Schema == nil || len(*resp.Schema) != len(resp.Data[0]) {
		return errors.New("query rows and schema do not match")
	}

	opts, err := queryResponseColumnTypeOptions(resp.Settings)
	if err != nil {
		return err
	}

	schema, err := parse_schema(resp.Schema, opts)
	if err != nil {
		return err
	}

	rows := make([][]driver.Value, 0, len(resp.Data))
	for _, rowText := range resp.Data {
		row := make([]driver.Value, len(rowText))
		for i, val := range rowText {
			if val == nil {
				row[i] = nil
				continue
			}
			parsed, err := schema.types[i].Parse(*val)
			if err != nil {
				return err
			}
			row[i] = parsed
		}
		rows = append(rows, row)
	}
	resp.typedRows = rows
	return nil
}

func prependQueryRows(resp *QueryResponse, prefixRows [][]driver.Value) {
	if resp == nil || len(prefixRows) == 0 {
		return
	}
	resp.typedRows = append(prefixRows, resp.typedRows...)
}
