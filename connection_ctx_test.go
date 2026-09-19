package godatabend

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeRequest is one statement or result-set close the fake server received.
type fakeRequest struct {
	sql       string // empty for a final (close) request
	path      string
	queryID   string
	userAgent string
}

// txnServer answers login, queries and final requests, tracking the
// transaction state the way Databend reports it in the session. Every query
// response carries a final URI so closing a result set is observable.
func txnServer(t *testing.T) (*httptest.Server, func() []fakeRequest) {
	t.Helper()
	var (
		mu       sync.Mutex
		requests []fakeRequest
	)
	record := func(r *http.Request, sql string) {
		mu.Lock()
		defer mu.Unlock()
		requests = append(requests, fakeRequest{
			sql: sql, path: r.URL.Path,
			queryID: r.Header.Get(DatabendQueryIDHeader), userAgent: r.Header.Get(UserAgent),
		})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/session/login":
			w.Header().Set(contentType, jsonMediaType)
			require.NoError(t, json.NewEncoder(w).Encode(LoginResponse{}))
		case r.URL.Path == "/v1/session/logout":
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/v1/query":
			var req QueryRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			record(r, req.SQL)

			state := TxnStateAutoCommit
			if strings.EqualFold(req.SQL, "BEGIN") {
				state = TxnStateActive
			}
			session := json.RawMessage(`{"txn_state":"` + string(state) + `"}`)
			w.Header().Set(contentType, jsonMediaType)
			require.NoError(t, json.NewEncoder(w).Encode(QueryResponse{
				ID: "q", State: "Succeeded", Session: &session,
				Schema: &[]DataField{}, Data: [][]*string{},
				FinalURI: "/v1/query/q/final",
			}))
		case strings.HasSuffix(r.URL.Path, "/final"):
			record(r, "")
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return server, func() []fakeRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]fakeRequest(nil), requests...)
	}
}

func statementsOf(requests []fakeRequest) []string {
	var out []string
	for _, r := range requests {
		if r.sql != "" {
			out = append(out, r.sql)
		}
	}
	return out
}

func requestFor(t *testing.T, requests []fakeRequest, sql string) fakeRequest {
	t.Helper()
	for _, r := range requests {
		if r.sql == sql {
			return r
		}
	}
	t.Fatalf("no %s request in %+v", sql, requests)
	return fakeRequest{}
}

// database/sql dials a connection with the context of the request that needed
// it and then keeps the connection in its pool. A transaction on that
// connection after the dialing request finished must not fail with the dialing
// request's cancellation.
func TestConnectionOutlivesItsDialContext(t *testing.T) {
	for _, end := range []string{"COMMIT", "ROLLBACK"} {
		t.Run(end, func(t *testing.T) {
			server, requests := txnServer(t)
			dialCtx, cancelDial := context.WithCancel(context.Background())
			dc, err := buildDatabendConn(dialCtx, testHTTPConfig(t, server.URL))
			require.NoError(t, err)
			t.Cleanup(func() { _ = dc.Close() })
			cancelDial()

			tx, err := dc.BeginTx(context.Background(), driver.TxOptions{})
			require.NoError(t, err)
			if end == "COMMIT" {
				require.NoError(t, tx.Commit())
			} else {
				require.NoError(t, tx.Rollback())
			}
			assert.Equal(t, []string{"BEGIN", end}, statementsOf(requests()))
		})
	}
}

// A dial deadline is the same trap as a cancellation: it expires long before a
// pooled connection is reused. The legacy context-free Begin uses the
// connection's own context.
func TestConnectionIgnoresTheDialDeadline(t *testing.T) {
	server, requests := txnServer(t)
	dialCtx, cancelDial := context.WithTimeout(context.Background(), time.Second)
	defer cancelDial()
	dc, err := buildDatabendConn(dialCtx, testHTTPConfig(t, server.URL))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dc.Close() })
	<-dialCtx.Done()

	tx, err := dc.Begin()
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	assert.Equal(t, []string{"BEGIN", "COMMIT"}, statementsOf(requests()))
}

// Nothing of the dialing request may stick to the pooled connection: not its
// cancellation, its query ID, or per-call values such as the user agent.
func TestConnectionRetainsNothingFromTheDialingRequest(t *testing.T) {
	server, _ := txnServer(t)
	dialCtx, cancelDial := context.WithCancel(context.WithValue(
		context.WithValue(context.Background(), ContextUserAgentID, "dial-request"),
		ContextKeyQueryID, "dial-query-id"))
	dc, err := buildDatabendConn(dialCtx, testHTTPConfig(t, server.URL))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dc.Close() })
	cancelDial()

	assert.NoError(t, dc.ctx.Err())
	assert.Nil(t, dc.ctx.Value(ContextUserAgentID))
	assert.Nil(t, dc.ctx.Value(ContextKeyQueryID))
}

// A transaction speaks for the request that began it. When another request
// dialed the connection, BEGIN and COMMIT must both carry the transaction's
// user agent, not the dialer's.
func TestTransactionKeepsItsOwnRequestIdentity(t *testing.T) {
	server, requests := txnServer(t)
	dialCtx := context.WithValue(context.Background(), ContextUserAgentID, "dial-request")
	dc, err := buildDatabendConn(dialCtx, testHTTPConfig(t, server.URL))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dc.Close() })

	txCtx := context.WithValue(context.Background(), ContextUserAgentID, "transaction-request")
	tx, err := dc.BeginTx(txCtx, driver.TxOptions{})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	got := requests()
	assert.Contains(t, requestFor(t, got, "BEGIN").userAgent, "(transaction-request)")
	assert.Contains(t, requestFor(t, got, "COMMIT").userAgent, "(transaction-request)")
}

// Databend treats a repeated query ID as a retry and returns the first query's
// result, so a COMMIT carrying the BEGIN's ID would report success without
// committing.
func TestTransactionDoesNotReuseTheBeginQueryID(t *testing.T) {
	server, requests := txnServer(t)
	callerCtx := context.WithValue(context.Background(), ContextKeyQueryID, "caller-query-id")
	dc, err := buildDatabendConn(callerCtx, testHTTPConfig(t, server.URL))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dc.Close() })

	tx, err := dc.BeginTx(callerCtx, driver.TxOptions{})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	got := requests()
	begin, commit := requestFor(t, got, "BEGIN"), requestFor(t, got, "COMMIT")
	assert.Equal(t, "caller-query-id", begin.queryID, "BEGIN runs on the caller's context")
	assert.NotEmpty(t, commit.queryID)
	assert.NotEqual(t, begin.queryID, commit.queryID, "COMMIT must carry its own query ID")
}

// database/sql rolls a transaction back when its BeginTx context is canceled,
// so that rollback must not inherit the cancellation.
func TestRollbackAfterTheTransactionContextIsCanceled(t *testing.T) {
	server, requests := txnServer(t)
	dc, err := buildDatabendConn(context.Background(), testHTTPConfig(t, server.URL))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dc.Close() })

	txCtx, cancelTx := context.WithCancel(context.Background())
	tx, err := dc.BeginTx(txCtx, driver.TxOptions{})
	require.NoError(t, err)
	cancelTx()

	require.NoError(t, tx.Rollback())
	assert.Equal(t, []string{"BEGIN", "ROLLBACK"}, statementsOf(requests()))
}

// database/sql closes a result set when its query context is canceled. The
// close must still reach the server, with the query's own metadata rather
// than that of whichever request dialed the connection.
func TestRowsCloseAfterTheQueryContextIsCanceled(t *testing.T) {
	server, requests := txnServer(t)
	dialCtx := context.WithValue(context.Background(), ContextUserAgentID, "dial-request")
	dc, err := buildDatabendConn(dialCtx, testHTTPConfig(t, server.URL))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dc.Close() })

	queryCtx, cancelQuery := context.WithCancel(
		context.WithValue(context.Background(), ContextUserAgentID, "query-request"))
	rows, err := dc.QueryContext(queryCtx, "SELECT 1", nil)
	require.NoError(t, err)
	cancelQuery()
	require.NoError(t, rows.Close())

	var final *fakeRequest
	for _, r := range requests() {
		if r.sql == "" {
			final = &r
		}
	}
	require.NotNil(t, final, "the result set must be finalized on the server")
	assert.Contains(t, final.userAgent, "(query-request)")
	assert.Equal(t, requestFor(t, requests(), "SELECT 1").queryID, final.queryID,
		"finalizing a query carries that query's ID")
}
