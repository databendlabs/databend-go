package godatabend

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"github.com/avast/retry-go"
	"github.com/databendcloud/bendsql/api/apierrors"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

type APIClient struct {
	UserEmail        string
	Password         string
	AccessToken      string
	RefreshToken     string
	ApiEndpoint      string
	Host             string
	CurrentOrgSlug   string
	CurrentWarehouse string
	AccountID        uint64
}

const (
	accept          = "Accept"
	authorization   = "Authorization"
	contentType     = "Content-Type"
	jsonContentType = "application/json; charset=utf-8"
	timeZone        = "Time-Zone"
	userAgent       = "User-Agent"
)

type DatabendConn struct {
	url                *url.URL
	ctx                context.Context
	cfg                *Config
	SQLState           string
	transport          *http.Transport
	cancel             context.CancelFunc
	closed             int32
	stmts              []*databendStmt
	txCtx              context.Context
	useDBLocation      bool
	useGzipCompression bool
	killQueryOnErr     bool
	killQueryTimeout   time.Duration
	logger             *log.Logger
	rest               *APIClient
}

func (dc *DatabendConn) exec(ctx context.Context, query string, args ...driver.Value) (driver.Result, error) {
	respCh := make(chan QueryResponse)
	errCh := make(chan error)
	go func() {
		err := dc.rest.QuerySync(ctx, query, args, respCh)
		errCh <- err
	}()

	for {
		select {
		case err := <-errCh:
			if err != nil {
				logrus.Errorf("error on query: %s", err)
				return emptyResult, err
			} else {
				return emptyResult, nil
			}
		case resp := <-respCh:
			b, err := json.Marshal(resp.Data)
			if err != nil {
				return emptyResult, err
			}
			_, _ = io.Copy(ioutil.Discard, bytes.NewReader(b))
		}
	}
}

func (dc *DatabendConn) query(ctx context.Context, query string, args []driver.Value) (driver.Rows, error) {
	var r0 *QueryResponse
	err := retry.Do(
		func() error {
			r, err := dc.rest.DoQuery(ctx, query, args)
			if err != nil {
				return fmt.Errorf("query failed: %w", err)
			}
			r0 = r
			return nil
		},
		// other err no need to retry
		retry.RetryIf(func(err error) bool {
			if err != nil && !(apierrors.IsProxyErr(err) || strings.Contains(err.Error(), apierrors.ProvisionWarehouseTimeout)) {
				return false
			}
			return true
		}),
		retry.Delay(2*time.Second),
		retry.Attempts(5),
	)
	if err != nil {
		return nil, fmt.Errorf("query failed after 5 retries: %w", err)
	}
	if r0.Error != nil {
		return nil, fmt.Errorf("query has error: %+v", r0.Error)
	}
	return newNextRows(dc, r0)
}

func (dc *DatabendConn) Begin() (driver.Tx, error) {
	return dc.BeginTx(dc.ctx, driver.TxOptions{})
}

func (dc *DatabendConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	logger.WithContext(ctx).Info("BeginTx")
	return &databendTx{dc}, nil
}

func (dc *DatabendConn) cleanup() {
	// must flush log buffer while the process is running.
	dc.rest = nil
	dc.cfg = nil
}

func (dc *DatabendConn) Prepare(query string) (driver.Stmt, error) {
	return dc.PrepareContext(context.Background(), query)
}

func (dc *DatabendConn) prepare(query string) (*databendStmt, error) {
	logger.WithContext(dc.ctx).Infoln("Prepare")
	if dc.rest == nil {
		return nil, driver.ErrBadConn
	}
	stmt := &databendStmt{
		dc:    dc,
		query: query,
	}
	return stmt, nil
}

func (dc *DatabendConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {

	return dc.prepare(query)
}

func buildDatabendConn(ctx context.Context, config Config) (*DatabendConn, error) {
	var logger *log.Logger
	if config.Debug {
		logger = log.New(os.Stderr, "databend: ", log.LstdFlags)
	}
	dc := &DatabendConn{
		url: config.url(map[string]string{"default_format": "TabSeparatedWithNamesAndTypes"}, false),
		ctx: ctx,
		cfg: &config,
		transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   config.Timeout,
				KeepAlive: config.IdleTimeout,
				DualStack: true,
			}).DialContext,
			MaxIdleConns:          1,
			IdleConnTimeout:       config.IdleTimeout,
			ResponseHeaderTimeout: config.ReadTimeout,
			TLSClientConfig:       getTLSConfigClone(config.TLSConfig),
		},
	}
	dc.rest = &APIClient{
		UserEmail:        dc.cfg.User,
		Password:         dc.cfg.Password,
		Host:             dc.cfg.Host,
		AccessToken:      dc.cfg.AccessToken,
		RefreshToken:     dc.cfg.RefreshToken,
		ApiEndpoint:      fmt.Sprintf("%s://%s", dc.cfg.Scheme, dc.cfg.Host),
		CurrentWarehouse: dc.cfg.Warehouse,
		CurrentOrgSlug:   dc.cfg.Org,
	}
	dc.logger = logger
	if dc.cfg.AccessToken != "" {
		return dc, nil
	}

	err := dc.rest.Login()
	if err != nil {
		return dc, err
	}
	dc.cfg.AccessToken = dc.rest.AccessToken
	dc.cfg.RefreshToken = dc.rest.RefreshToken
	return dc, nil
}

func (c *DatabendConn) log(msg ...interface{}) {
	if c.logger != nil {
		c.logger.Println(msg...)
	}
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
func (dc *DatabendConn) Close() error {
	if atomic.CompareAndSwapInt32(&dc.closed, 0, 1) {
		dc.log("close connection", dc.url.Scheme, dc.url.Host, dc.url.Path)
		cancel := dc.cancel
		transport := dc.transport
		dc.transport = nil
		dc.cancel = nil

		if cancel != nil {
			cancel()
		}
		if transport != nil {
			transport.CloseIdleConnections()
		}
		dc.cleanup()
	}
	return nil
}

func (dc *DatabendConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return dc.exec(context.Background(), query, args)
}

func (dc *DatabendConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return dc.query(context.Background(), query, args)
}

// Commit applies prepared statement if it exists
func (dc *DatabendConn) Commit() (err error) {
	if atomic.LoadInt32(&dc.closed) != 0 {
		return driver.ErrBadConn
	}
	if dc.txCtx == nil {
		return sql.ErrTxDone
	}
	ctx := dc.txCtx
	stmts := dc.stmts
	dc.txCtx = nil
	dc.stmts = stmts[:0]

	if len(stmts) == 0 {
		return nil
	}
	for _, stmt := range stmts {
		dc.log("commit statement: ", stmt.prefix, stmt.pattern)
		if err = stmt.commit(ctx); err != nil {
			break
		}
	}
	return
}

// Rollback cleans prepared statement
func (dc *DatabendConn) Rollback() error {
	if atomic.LoadInt32(&dc.closed) != 0 {
		return driver.ErrBadConn
	}
	if dc.txCtx == nil {
		return sql.ErrTxDone
	}
	dc.txCtx = nil
	stmts := dc.stmts
	dc.stmts = stmts[:0]

	if len(stmts) == 0 {
		// there is no statements, so nothing to rollback
		return sql.ErrTxDone
	}
	// the statements will be closed by sql.Tx
	return nil
}
