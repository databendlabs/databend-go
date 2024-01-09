package godatabend

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/avast/retry-go"
	"github.com/google/uuid"
)

const (
	accept          = "Accept"
	authorization   = "Authorization"
	contentType     = "Content-Type"
	jsonContentType = "application/json; charset=utf-8"
)

type DatabendConn struct {
	ctx    context.Context
	cfg    *Config
	cancel context.CancelFunc
	closed int32
	stmts  []*databendStmt
	logger *log.Logger
	rest   *APIClient
	commit func() error
}

func (dc *DatabendConn) exec(ctx context.Context, query string, args ...driver.Value) (driver.Result, error) {
	respCh := make(chan QueryResponse)
	errCh := make(chan error)
	ctx = checkQueryID(ctx)

	go func() {
		err := dc.rest.QuerySync(ctx, query, args, respCh)
		errCh <- err
	}()

	for {
		select {
		case err := <-errCh:
			if err != nil {
				return emptyResult, err
			} else {
				return emptyResult, nil
			}
		case resp := <-respCh:
			b, err := json.Marshal(resp.Data)
			if err != nil {
				return emptyResult, err
			}
			_, _ = io.Copy(io.Discard, bytes.NewReader(b))
		}
	}
}

func (dc *DatabendConn) query(ctx context.Context, query string, args ...driver.Value) (driver.Rows, error) {
	var r0 *QueryResponse
	ctx = checkQueryID(ctx)
	err := retry.Do(
		func() error {
			r, err := dc.rest.DoQuery(ctx, query, args)
			if err != nil {
				return err
			}
			r0 = r
			return nil
		},
		// other err no need to retry
		retry.RetryIf(func(err error) bool {
			if err != nil && (IsProxyErr(err) || strings.Contains(err.Error(), ProvisionWarehouseTimeout)) {
				return true
			}
			return false
		}),
		retry.Delay(2*time.Second),
		retry.Attempts(5),
		retry.DelayType(retry.FixedDelay),
	)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	if r0.Error != nil {
		return nil, fmt.Errorf("query has error: %+v", r0.Error)
	}
	return newNextRows(ctx, dc, r0)
}

//func (dc *DatabendConn) Begin() (driver.Tx, error) {
//	return dc.BeginTx(dc.ctx, driver.TxOptions{})
//}

func (dc *DatabendConn) Begin() (driver.Tx, error) { return dc, nil }

func (dc *DatabendConn) cleanup() {
	// must flush log buffer while the process is running.
	dc.rest = nil
	dc.cfg = nil
}

func (dc *DatabendConn) Ping(ctx context.Context) error {
	_, err := dc.exec(ctx, "SELECT 1")
	if err != nil {
		return err
	}
	return nil
}

func (dc *DatabendConn) Prepare(query string) (driver.Stmt, error) {
	return dc.PrepareContext(dc.ctx, query)
}

func (dc *DatabendConn) prepare(ctx context.Context, query string) (*databendStmt, error) {
	logger.WithContext(dc.ctx).Infoln("Prepare")
	if dc.rest == nil {
		return nil, driver.ErrBadConn
	}
	batch, err := dc.prepareBatch(ctx, query)
	if err != nil {
		return nil, err
	}
	dc.commit = batch.BatchInsert
	stmt := &databendStmt{
		dc:    dc,
		query: query,
		batch: batch,
	}
	return stmt, nil
}

func (dc *DatabendConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	ctx = checkQueryID(ctx)
	return dc.prepare(ctx, query)
}

func buildDatabendConn(ctx context.Context, config Config) (*DatabendConn, error) {
	dc := &DatabendConn{
		ctx:  ctx,
		cfg:  &config,
		rest: NewAPIClientFromConfig(&config),
	}
	if config.Debug {
		dc.logger = log.New(os.Stderr, "databend: ", log.LstdFlags)
	}
	return dc, nil
}

func (dc *DatabendConn) log(msg ...interface{}) {
	if dc.logger != nil {
		dc.logger.Println(msg...)
	}
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
func (dc *DatabendConn) Close() error {
	if atomic.CompareAndSwapInt32(&dc.closed, 0, 1) {
		dc.log("close connection", dc.rest)
		cancel := dc.cancel
		dc.cancel = nil

		if cancel != nil {
			cancel()
		}
		dc.cleanup()
	}
	return nil
}

func (dc *DatabendConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	values := make([]driver.Value, len(args))
	for i, arg := range args {
		values[i] = arg.Value
	}
	return dc.exec(ctx, query, values...)
}

func (dc *DatabendConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	values := make([]driver.Value, len(args))
	for i, arg := range args {
		values[i] = arg.Value
	}
	return dc.query(ctx, query, values...)
}

// Commit applies prepared statement if it exists
func (dc *DatabendConn) Commit() (err error) {
	if dc.commit == nil {
		return nil
	}
	defer func() {
		dc.commit = nil
	}()
	return dc.commit()
}

// Rollback cleans prepared statement
func (dc *DatabendConn) Rollback() error {
	dc.commit = nil
	dc.Close()
	return nil
}

// checkQueryID checks if query_id exists in context, if not, generate a new one
func checkQueryID(ctx context.Context) context.Context {
	if _, ok := ctx.Value(ContextKeyQueryID).(string); ok {
		return ctx
	}
	queryId := uuid.NewString()
	ctx = context.WithValue(ctx, ContextKeyQueryID, queryId)
	return ctx
}
