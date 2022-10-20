package godatabend

import (
	"bytes"
	"context"
	"database/sql/driver"
	"regexp"
	"strings"
	"sync/atomic"
)

var (
	splitInsertRe = regexp.MustCompile(`(?si)(.+\s*VALUES)\s*(\(.+\))`)
)

type databendStmt struct {
	dc        *DatabendConn
	closed    int32
	prefix    string
	pattern   string
	index     []int
	batchMode bool
	args      [][]driver.Value
	query     string
}

func newStmt(query string) *databendStmt {
	s := &databendStmt{pattern: query}
	index := splitInsertRe.FindStringSubmatchIndex(strings.ToUpper(query))
	if len(index) == 6 {
		s.prefix = query[index[2]:index[3]]
		s.pattern = query[index[4]:index[5]]
		s.batchMode = true
	}
	s.index = placeholders(s.pattern)
	if len(s.index) == 0 {
		s.batchMode = false
	}
	return s
}

func (stmt *databendStmt) Close() error {
	logger.WithContext(stmt.dc.ctx).Infoln("Stmt.Close")
	return nil
}

func (stmt *databendStmt) NumInput() int {
	logger.WithContext(stmt.dc.ctx).Infoln("Stmt.NumInput")
	return -1
}

func (stmt *databendStmt) Exec(args []driver.Value) (driver.Result, error) {
	logger.WithContext(stmt.dc.ctx).Infoln("Stmt.Exec")
	return stmt.dc.Exec(stmt.query, args)
}

func (stmt *databendStmt) Query(args []driver.Value) (driver.Rows, error) {
	logger.WithContext(stmt.dc.ctx).Infoln("Stmt.Query")
	return stmt.dc.Query(stmt.query, args)
}

func (stmt *databendStmt) commit(ctx context.Context) error {
	if atomic.CompareAndSwapInt32(&stmt.closed, 0, 1) {
		// statement is not usable after commit
		// this code will not run if statement has been closed
		args := stmt.args
		con := stmt.dc
		stmt.args = nil
		stmt.dc = nil
		if len(args) == 0 {
			return nil
		}
		buf := bytes.NewBufferString(stmt.prefix)
		var (
			p   string
			err error
		)
		for i, arg := range args {
			if i > 0 {
				buf.WriteString(", ")
			}
			if p, err = interpolateParams2(stmt.pattern, arg, stmt.index); err != nil {
				return err
			}
			buf.WriteString(p)
		}
		_, err = con.exec(ctx, buf.String(), nil)
		return err
	}
	return nil
}
