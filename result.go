package godatabend

import (
	"database/sql/driver"

	"github.com/pkg/errors"
)

type databendResult struct {
	affectedRows int64
	insertId     int64
}

func newDatabendResult(affectedRows, insertId int64) *databendResult {
	return &databendResult{
		affectedRows: affectedRows,
		insertId:     insertId,
	}
}

func (res *databendResult) LastInsertId() (int64, error) {
	return res.insertId, errors.New("LastInsertId is not supported")
}

func (res *databendResult) RowsAffected() (int64, error) {
	return res.affectedRows, nil
}

var emptyResult driver.Result = noResult{}

type noResult struct{}

func (noResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (noResult) RowsAffected() (int64, error) {
	return 0, nil
}
