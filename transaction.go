package godatabend

import (
	"database/sql/driver"
)

type databendTx struct {
	dc *DatabendConn
}

func (tx *databendTx) Commit() (err error) {
	if tx.dc == nil || tx.dc.rest == nil {
		return driver.ErrBadConn
	}
	defer func() {
		tx.dc.batchInsert = nil
	}()
	if tx.dc.batchMode && tx.dc.batchInsert != nil {
		err = tx.dc.batchInsert()
		if err != nil {
			return
		}
	}
	_, err = tx.dc.exec(tx.dc.ctx, "COMMIT")
	if err != nil {
		return
	}
	tx.dc = nil
	return
}

func (tx *databendTx) Rollback() (err error) {
	if tx.dc == nil || tx.dc.rest == nil {
		return driver.ErrBadConn
	}
	_, err = tx.dc.exec(tx.dc.ctx, "ROLLBACK")
	if err != nil {
		return
	}
	tx.dc = nil
	return
}
