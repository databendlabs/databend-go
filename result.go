package godatabend

import "database/sql/driver"

var emptyResult driver.Result = noResult{}

type noResult struct{}

func (noResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (noResult) RowsAffected() (int64, error) {
	return 0, nil
}
