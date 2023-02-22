package driver

import "database/sql/driver"

type Batch interface {
	AppendToFile(v []driver.Value) error
	UploadToStage() (string, error)
	BatchInsert() error
}
