package godatabend

import (
	"context"
	"fmt"
	"github.com/databendcloud/databend-go/lib/driver"
	"github.com/pkg/errors"
	"regexp"
	"strings"
)

// \x60 represents a backtick
var httpInsertRe = regexp.MustCompile(`(?i)^INSERT INTO\s+\x60?([\w.^\(]+)\x60?\s*(\([^\)]*\))?`)

func (dc *DatabendConn) prepareBatch(ctx context.Context, query string) (driver.Batch, error) {
	matches := httpInsertRe.FindStringSubmatch(query)
	if len(matches) < 3 {
		return nil, errors.New("cannot get table name from query")
	}
	tableName := matches[1]
	var rColumns []string
	if matches[2] != "" {
		colMatch := strings.TrimSuffix(strings.TrimPrefix(matches[2], "("), ")")
		rColumns = strings.Split(colMatch, ",")
		for i := range rColumns {
			rColumns[i] = strings.TrimSpace(rColumns[i])
		}
	}
	query = "INSERT INTO " + tableName
	queryTableSchema := "DESCRIBE TABLE " + tableName

	r, err := dc.rest.DoQuery(ctx, queryTableSchema, nil)
	if err != nil {
		return nil, err
	}
	// get Table columns and types
	var columnNames, columnTypes []string
	for i := range r.Data {
		if len(r.Data[i]) > 1 {
			columnNames = append(columnNames, fmt.Sprintf("%s", r.Data[i][0]))
			columnTypes = append(columnTypes, fmt.Sprintf("%s", r.Data[i][1]))
		}
	}

	return &httpBatch{
		query:       query,
		ctx:         ctx,
		conn:        dc,
		columnNames: columnNames,
		columnTypes: columnTypes,
	}, nil
}

type httpBatch struct {
	query       string
	err         error
	ctx         context.Context
	conn        *DatabendConn
	columnNames []string
	columnTypes []string
}

func (b *httpBatch) UpToStage(v ...interface{}) error {
	// generate parquet file and upload to stage ~
	return nil
}

func (b *httpBatch) CopyInto() error {
	// copy into db.table from @~/xx.parquet
	return nil
}

var _ driver.Batch = (*httpBatch)(nil)
