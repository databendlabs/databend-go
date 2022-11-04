package godatabend

import (
	"context"
	"database/sql/driver"
	"regexp"
	"strings"
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
	//1. trans args to parquet file

	//2. /v1/upload_to_stage parquet file

	// 3. copy into db.table from @~/parquet

	// 4. delete the file ?

	return stmt.dc.Exec(stmt.query, args)
}

func (stmt *databendStmt) Query(args []driver.Value) (driver.Rows, error) {
	logger.WithContext(stmt.dc.ctx).Infoln("Stmt.Query")
	return stmt.dc.Query(stmt.query, args)
}

func (stmt *databendStmt) commit(ctx context.Context) error {
	logger.WithContext(stmt.dc.ctx).Infoln("Stmt Commit")

	return nil
}
