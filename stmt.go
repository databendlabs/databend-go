package godatabend

import (
	"context"
	"database/sql/driver"
	"errors"
)

var (
	errStmtCosed = errors.New("stmt is already closed")
)

type databendStmt struct {
	dc           *DatabendConn
	query        string
	placeholders []int
	closed       bool
}

func (stmt *databendStmt) Close() error {
	stmt.closed = true
	return nil
}

func (stmt *databendStmt) NumInput() int {
	return len(stmt.placeholders)
}

func (stmt *databendStmt) Exec(args []driver.Value) (driver.Result, error) {
	if stmt.closed {
		return nil, errStmtCosed
	}
	return stmt.dc.exec(context.Background(), stmt.query, &stmt.placeholders, args)
}

func (stmt *databendStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if stmt.closed {
		return nil, errStmtCosed
	}
	values := make([]driver.Value, len(args))
	for i, arg := range args {
		values[i] = arg.Value
	}
	return stmt.dc.exec(ctx, stmt.query, &stmt.placeholders, values)
}

func (stmt *databendStmt) Query(args []driver.Value) (driver.Rows, error) {
	if stmt.closed {
		return nil, errStmtCosed
	}
	return stmt.dc.query(context.Background(), stmt.query, &stmt.placeholders, args)
}

func (stmt *databendStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if stmt.closed {
		return nil, errStmtCosed
	}
	values := make([]driver.Value, len(args))
	for i, arg := range args {
		values[i] = arg.Value
	}
	return stmt.dc.query(ctx, stmt.query, &stmt.placeholders, values)
}
