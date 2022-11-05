package godatabend

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/databendcloud/databend-go/lib/driver"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"os"
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

	csvFileName := fmt.Sprintf("%s.csv", uuid.NewString())

	csvFile, err := os.OpenFile(csvFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	defer csvFile.Close()
	writer := csv.NewWriter(csvFile)
	err = writer.Write(columnNames)
	if err != nil {
		return nil, err
	}
	writer.Flush()

	return &httpBatch{
		query:       query,
		ctx:         ctx,
		conn:        dc,
		columnNames: columnNames,
		columnTypes: columnTypes,
		batchFile:   csvFileName,
	}, nil
}

type httpBatch struct {
	query       string
	err         error
	ctx         context.Context
	conn        *DatabendConn
	columnNames []string
	columnTypes []string
	batchFile   string
}

func (b *httpBatch) CopyInto() error {
	// copy into db.table from @~/xx.parquet
	return nil
}

func (b *httpBatch) AppendToFile(v ...interface{}) error {
	csvFile, err := os.OpenFile(b.batchFile, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer csvFile.Close()

	var lineData []string
	for _, d := range v {
		lineData = append(lineData, fmt.Sprintf("%s", d))
	}
	writer := csv.NewWriter(csvFile)
	err = writer.Write(lineData)
	if err != nil {
		return err
	}
	writer.Flush()

	return nil
}

func (b *httpBatch) UploadToStage() error {

	return b.conn.rest.uploadToStage(b.batchFile)
}

var _ driver.Batch = (*httpBatch)(nil)
