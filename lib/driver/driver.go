package driver

import "database/sql/driver"

type Batch interface {
	AppendToFile(v []driver.Value) error
	BatchInsert() error
}
