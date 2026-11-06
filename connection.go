package godatabend

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

const (
	accept                 = "Accept"
	contentType            = "Content-Type"
	jsonContentType        = "application/json; charset=utf-8"
	jsonMediaType          = "application/json"
	arrowStreamContentType = "application/vnd.apache.arrow.stream"
)

var _ driver.Validator = (*DatabendConn)(nil)

type DatabendConn struct {
	ctx    context.Context
	cfg    *Config
	cancel context.CancelFunc
	closed int32
	logger *log.Logger
	rest   *APIClient
}

func (dc *DatabendConn) columnTypeOptions(settings *Settings, location *time.Location) *ColumnTypeOptions {
	opts := defaultColumnTypeOptions()
	if location != nil {
		opts.SetTimezone(location)
	} else if dc.cfg.Location != nil {
		opts.SetTimezone(dc.cfg.Location)
	}
	if dc.cfg != nil && dc.cfg.Params != nil {
		if format, ok := dc.cfg.Params["geometry_output_format"]; ok {
			opts.SetGeometryOutputFormat(format)
		}
		if format, ok := dc.cfg.Params["binary_output_format"]; ok {
			opts.SetBinaryOutputFormat(format)
		}
		if mode, ok := dc.cfg.Params["http_json_result_mode"]; ok {
			opts.SetHTTPJSONResultMode(mode)
		}
	}
	if settings != nil {
		opts.SetGeometryOutputFormat(settings.GeometryOutputFormat)
		opts.SetBinaryOutputFormat(settings.BinaryOutputFormat)
		opts.SetHTTPJSONResultMode(settings.HTTPJSONResultMode)
	}
	return opts
}

// badConnIfInvalidated wraps ErrSessionInvalidated into driver.ErrBadConn.
// Both stay in the chain: database/sql only ever tests
// errors.Is(err, driver.ErrBadConn), and callers can still match
// ErrSessionInvalidated or read the cause and ids from the message instead of
// a bare "driver: bad connection".
//
// database/sql does not retry ErrBadConn on a connection the caller holds
// (sql.Conn, sql.Tx); it marks it closed, which is what we want. For pooled
// db.Exec/db.Query the connection is normally already gone via IsValid, and
// a retry on a fresh connection is harmless because nothing was sent.
func badConnIfInvalidated(err error) error {
	if errors.Is(err, ErrSessionInvalidated) {
		return fmt.Errorf("%w: %w", driver.ErrBadConn, err)
	}
	return err
}

func (dc *DatabendConn) exec(ctx context.Context, query string, placeholders *[]int, args []driver.Value) (driver.Result, error) {
	if err := dc.rest.checkSessionValid(); err != nil {
		return emptyResult, badConnIfInvalidated(err)
	}
	dc.rest.checkoutStmts++
	ctx = checkQueryID(ctx)
	query, err := buildQuery(query, args, placeholders)
	if err != nil {
		return emptyResult, err
	}
	queryResponse, err := dc.rest.QuerySync(ctx, query)
	if err != nil {
		return emptyResult, err
	}

	affectedRows, err := parseAffectedRows(queryResponse)
	if err != nil {
		return emptyResult, err
	}

	return newDatabendResult(affectedRows, 0), nil
}

func (dc *DatabendConn) query(ctx context.Context, query string, placeholders *[]int, args []driver.Value) (rows driver.Rows, err error) {
	if err := dc.rest.checkSessionValid(); err != nil {
		return nil, badConnIfInvalidated(err)
	}
	dc.rest.checkoutStmts++
	ctx = checkQueryID(ctx)
	query, err = buildQuery(query, args, placeholders)
	if err != nil {
		return nil, err
	}
	r0, err := dc.rest.StartQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer func() {
		if err != nil {
			_ = dc.rest.CloseQuery(ctx, r0)
		}
	}()

	if r0.Error != nil {
		return nil, fmt.Errorf("query error: %+v", r0.Error)
	}
	response, err := waitForData(ctx, dc, r0)
	if err != nil {
		return nil, err
	}
	return dc.newNextRows(ctx, response)
}

func (dc *DatabendConn) Begin() (driver.Tx, error) {
	return dc.BeginTx(dc.ctx, driver.TxOptions{})
}

func (dc *DatabendConn) BeginTx(
	ctx context.Context,
	_ driver.TxOptions) (
	driver.Tx, error) {
	if dc.rest == nil {
		return nil, driver.ErrBadConn
	}
	if _, err := dc.exec(ctx, "BEGIN", nil, nil); err != nil {
		return nil, err
	}
	return &databendTx{dc: dc, ctx: txContext(ctx)}, nil
}

func (dc *DatabendConn) cleanup() {
	_ = dc.rest.Logout(dc.ctx)
	dc.rest = nil
	dc.cfg = nil
}

func (dc *DatabendConn) Ping(ctx context.Context) error {
	// /v1/verify only checks authentication and would report a connection
	// with lost session state as healthy. database/sql retries ErrBadConn
	// from Ping on another connection.
	if err := dc.rest.checkSessionValid(); err != nil {
		return badConnIfInvalidated(err)
	}
	err := dc.rest.Verify(ctx)
	if err != nil {
		return errors.Wrap(err, "ping failed")
	}
	return nil
}

func (dc *DatabendConn) Prepare(query string) (driver.Stmt, error) {
	return dc.PrepareContext(dc.ctx, query)
}

func (dc *DatabendConn) prepare(_ context.Context, query string) (*databendStmt, error) {
	logger.WithContext(dc.ctx).Infoln("Prepare")
	if dc.rest == nil {
		return nil, driver.ErrBadConn
	}
	placeholders := placeholders(query)
	stmt := &databendStmt{
		dc:           dc,
		query:        query,
		placeholders: placeholders,
	}
	return stmt, nil
}

func (dc *DatabendConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	ctx = checkQueryID(ctx)
	return dc.prepare(ctx, query)
}

func buildDatabendConn(ctx context.Context, config *Config) (*DatabendConn, error) {
	dc := &DatabendConn{
		// database/sql dials a pooled connection with the context of whichever
		// request needed it, and the connection then outlives that request.
		// Nothing of that request may stick to the connection: its
		// cancellation or deadline would fail later calls on a healthy
		// connection, its query ID would make Databend treat a later statement
		// as a retry of the dialing one, and its per-call values (such as the
		// user agent override) would be attributed to unrelated requests.
		// dc.ctx is only for calls that belong to no request (logout and the
		// legacy context-free Begin/Prepare); the dial context is used for the
		// dial itself below. Connection-level settings come from Config.
		ctx:  context.Background(),
		cfg:  config,
		rest: NewAPIClientFromConfig(config),
	}
	if config.Debug {
		dc.logger = log.New(os.Stderr, "databend: ", log.LstdFlags)
	}
	if err := dc.rest.initializeConnectionInfo(ctx); err != nil {
		_ = dc.rest.Logout(context.Background())
		return nil, err
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

// IsValid implements driver.Validator. database/sql calls it when a connection
// is returned to the pool (putConn -> validateConnection) and closes the
// connection instead of pooling it when this returns false. So the connection
// that hit the error is dropped the moment the failing call releases it and
// never reaches the free list. A connection whose server-side session state
// (sticky node, temp tables, transaction) is gone must not be reused: without
// this the pool would keep returning the same broken connection forever.
func (dc *DatabendConn) IsValid() bool {
	if atomic.LoadInt32(&dc.closed) != 0 || dc.rest == nil {
		return false
	}
	// Called on every return to the pool, so it marks the end of a checkout.
	dc.rest.checkoutStmts = 0
	return !dc.rest.SessionInvalidated()
}

func (dc *DatabendConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	values := make([]driver.Value, len(args))
	for i, arg := range args {
		values[i] = arg.Value
	}
	return dc.exec(ctx, query, nil, values)
}

func (dc *DatabendConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	values := make([]driver.Value, len(args))
	for i, arg := range args {
		values[i] = arg.Value
	}
	return dc.query(ctx, query, nil, values)
}

func (dc *DatabendConn) ExecBatch(ctx context.Context, query string, rows [][]driver.Value) (driver.Result, error) {
	if err := dc.rest.checkSessionValid(); err != nil {
		return nil, badConnIfInvalidated(err)
	}
	dc.rest.checkoutStmts++
	batch, err := dc.prepareBatch(ctx, query)
	if err != nil {
		return nil, err
	}
	for _, args := range rows {
		err = batch.AppendToFile(args)
		if err != nil {
			return nil, err
		}
	}
	err = batch.BatchInsert()
	if err != nil {
		return nil, err
	}
	return newDatabendResult(int64(len(rows)), 0), nil
}

// checkQueryID checks if query_id exists in context, if not, generate a new one
func checkQueryID(ctx context.Context) context.Context {
	if _, ok := ctx.Value(ContextKeyQueryID).(string); !ok {
		queryId := uuid.NewString()
		queryId = strings.ReplaceAll(queryId, "-", "")
		ctx = context.WithValue(ctx, ContextKeyQueryID, queryId)
	}
	return ctx
}
