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

// txnServer answers login and queries, tracking the transaction state the way
// Databend reports it in the session, and records every statement it receives.
func txnServer(t *testing.T) (*httptest.Server, func() []string) {
	server, statements, _ := txnServerWithIDs(t)
	return server, statements
}

// txnServerWithIDs also returns the query ID header of every statement.
func txnServerWithIDs(t *testing.T) (*httptest.Server, func() []string, func() []string) {
	t.Helper()
	var (
		mu  sync.Mutex
		sql []string
		ids []string
	)
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
			mu.Lock()
			sql = append(sql, req.SQL)
			ids = append(ids, r.Header.Get(DatabendQueryIDHeader))
			mu.Unlock()

			state := TxnStateAutoCommit
			if strings.EqualFold(req.SQL, "BEGIN") {
				state = TxnStateActive
			}
			session := json.RawMessage(`{"txn_state":"` + string(state) + `"}`)
			w.Header().Set(contentType, jsonMediaType)
			require.NoError(t, json.NewEncoder(w).Encode(QueryResponse{
				ID: "q", State: "Succeeded", Session: &session,
				Schema: &[]DataField{}, Data: [][]*string{},
			}))
		case strings.HasPrefix(r.URL.Path, "/v1/query/"):
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return server, func() []string {
			mu.Lock()
			defer mu.Unlock()
			return append([]string(nil), sql...)
		}, func() []string {
			mu.Lock()
			defer mu.Unlock()
			return append([]string(nil), ids...)
		}
}

// database/sql dials a connection with the context of the request that needed
// it and then keeps the connection in its pool. Committing on that connection
// after the dialing request finished must not fail with the dialing request's
// cancellation.
func TestConnectionOutlivesItsDialContext(t *testing.T) {
	for _, end := range []string{"COMMIT", "ROLLBACK"} {
		t.Run(end, func(t *testing.T) {
			server, statements := txnServer(t)
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
			assert.Equal(t, []string{"BEGIN", end}, statements())
		})
	}
}

// A dial deadline is the same trap as a cancellation: it expires long before a
// pooled connection is reused.
func TestConnectionIgnoresTheDialDeadline(t *testing.T) {
	server, statements := txnServer(t)
	dialCtx, cancelDial := context.WithTimeout(context.Background(), time.Second)
	defer cancelDial()
	dc, err := buildDatabendConn(dialCtx, testHTTPConfig(t, server.URL))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dc.Close() })
	<-dialCtx.Done()

	tx, err := dc.Begin()
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	assert.Equal(t, []string{"BEGIN", "COMMIT"}, statements())
}

// The dial context's values, such as the propagated user agent, still apply to
// the calls that use the connection's context.
func TestConnectionKeepsTheDialContextValues(t *testing.T) {
	server, _ := txnServer(t)
	dialCtx, cancelDial := context.WithCancel(
		context.WithValue(context.Background(), ContextUserAgentID, "my-app/1.0"))
	dc, err := buildDatabendConn(dialCtx, testHTTPConfig(t, server.URL))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dc.Close() })
	cancelDial()

	assert.NoError(t, dc.ctx.Err())
	assert.Equal(t, "my-app/1.0", dc.ctx.Value(ContextUserAgentID))
}

// A connection dialed by a request that set its own query ID must not reuse
// that ID for later calls. Databend treats a repeated query ID as a retry and
// returns the first query's result, so a COMMIT carrying the BEGIN's ID would
// report success without committing.
func TestConnectionDoesNotReuseTheDialQueryID(t *testing.T) {
	server, statements, ids := txnServerWithIDs(t)
	callerCtx := context.WithValue(context.Background(), ContextKeyQueryID, "caller-query-id")
	dc, err := buildDatabendConn(callerCtx, testHTTPConfig(t, server.URL))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dc.Close() })

	tx, err := dc.BeginTx(callerCtx, driver.TxOptions{})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	require.Equal(t, []string{"BEGIN", "COMMIT"}, statements())
	got := ids()
	assert.Equal(t, "caller-query-id", got[0], "BEGIN runs on the caller's context")
	assert.NotEmpty(t, got[1])
	assert.NotEqual(t, got[0], got[1], "COMMIT must carry its own query ID")
	assert.Nil(t, dc.ctx.Value(ContextKeyQueryID))
}
