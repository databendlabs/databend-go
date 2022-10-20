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
	_, err = tx.dc.exec(tx.dc.ctx, "COMMIT", nil)
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
	_, err = tx.dc.exec(tx.dc.ctx, "ROLLBACK", nil)
	if err != nil {
		return
	}
	tx.dc = nil
	return
}
