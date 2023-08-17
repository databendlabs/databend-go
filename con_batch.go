package godatabend

import (
	"bufio"
	"context"
	"database/sql/driver"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"

	ldriver "github.com/databendcloud/databend-go/lib/driver"
)

// \x60 represents a backtick
var httpInsertRe = regexp.MustCompile(`(?i)^INSERT INTO\s+\x60?([\w.^\(]+)\x60?\s*(\([^\)]*\))? VALUES`)

func (dc *DatabendConn) prepareBatch(ctx context.Context, query string) (ldriver.Batch, error) {
	matches := httpInsertRe.FindStringSubmatch(query)
	if len(matches) < 2 {
		return nil, errors.New("cannot get table name from query")
	}
	csvFileName := fmt.Sprintf("%s/%s.csv", os.TempDir(), uuid.NewString())

	csvFile, err := os.OpenFile(csvFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	defer csvFile.Close()
	writer := csv.NewWriter(csvFile)
	writer.Flush()

	return &httpBatch{
		query:     query,
		ctx:       ctx,
		conn:      dc,
		batchFile: csvFileName,
	}, nil
}

type httpBatch struct {
	query     string
	ctx       context.Context
	conn      *DatabendConn
	batchFile string
	err       error
}

func (b *httpBatch) BatchInsert() error {
	defer func() {
		err := os.RemoveAll(b.batchFile)
		if err != nil {
			b.conn.log("delete batch insert file failed: ", err)
		}
	}()
	stage, err := b.UploadToStage()
	if err != nil {
		return errors.Wrap(err, "upload to stage failed")
	}
	_, err = b.conn.rest.InsertWithStage(b.query, stage, nil, nil)
	if err != nil {
		return errors.Wrap(err, "insert with stage failed")
	}
	return nil
}

func (b *httpBatch) AppendToFile(v []driver.Value) error {
	csvFile, err := os.OpenFile(b.batchFile, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer csvFile.Close()

	lineData := make([]string, 0, len(v))
	for i := range v {
		lineData = append(lineData, fmt.Sprintf("%v", v[i]))
	}
	writer := csv.NewWriter(csvFile)
	err = writer.Write(lineData)
	if err != nil {
		return err
	}
	writer.Flush()

	return nil
}

func (b *httpBatch) UploadToStage() (*StageLocation, error) {
	fi, err := os.Stat(b.batchFile)
	if err != nil {
		return nil, errors.Wrap(err, "get batch file size failed")
	}
	size := fi.Size()

	f, err := os.Open(b.batchFile)
	if err != nil {
		return nil, errors.Wrap(err, "open batch file failed")
	}
	defer f.Close()
	input := bufio.NewReader(f)
	stage := &StageLocation{
		Name: "~",
		Path: fmt.Sprintf("batch/%d-%s", time.Now().Unix(), filepath.Base(b.batchFile)),
	}
	return stage, b.conn.rest.UploadToStage(stage, input, size)
}

var _ ldriver.Batch = (*httpBatch)(nil)
