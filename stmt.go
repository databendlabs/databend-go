package godatabend

import (
	"database/sql/driver"

	"github.com/pkg/errors"
)

type databendStmt struct {
	dc      *DatabendConn
	closed  int32
	prefix  string
	pattern string
	index   []int
	args    [][]driver.Value
	query   string
	batch   Batch
}

func (stmt *databendStmt) Close() error {
	logger.WithContext(stmt.dc.ctx).Infoln("Stmt.Close")
	return stmt.dc.Close()
}

func (stmt *databendStmt) NumInput() int {
	logger.WithContext(stmt.dc.ctx).Infoln("Stmt.NumInput")
	return -1
}

func (stmt *databendStmt) Exec(args []driver.Value) (driver.Result, error) {
	//1. trans args to csv file
	err := stmt.batch.AppendToFile(args)
	if err != nil {
		return nil, err
	}

	return driver.RowsAffected(0), nil
}

//func (stmt *databendStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
//	values := make([]driver.Value, 0, len(args))
//	for _, v := range args {
//		values = append(values, v.Value)
//	}
//	return stmt.Exec(values)
//}

func (stmt *databendStmt) Query(args []driver.Value) (driver.Rows, error) {
	logger.WithContext(stmt.dc.ctx).Infoln("Stmt.Query")
	return nil, errors.New("only Exec method supported in batch mode")
}
