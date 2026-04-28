package godatabend

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildDatabendConnInitializesViaLogin(t *testing.T) {
	var (
		mu          sync.Mutex
		loginCount  int
		logoutCount int
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/session/login":
			mu.Lock()
			loginCount++
			mu.Unlock()

			w.Header().Set(contentType, jsonMediaType)
			w.Header().Set(DatabendSessionIDHeader, "session-login")
			require.NoError(t, json.NewEncoder(w).Encode(LoginResponse{Version: "Databend Query v1.2.899-nightly"}))
		case "/v1/session/logout":
			mu.Lock()
			logoutCount++
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	cfg := testHTTPConfig(t, server.URL)
	dc, err := buildDatabendConn(context.Background(), cfg)
	require.NoError(t, err)

	assert.Equal(t, "session-login", dc.rest.SessionID)
	assert.True(t, dc.rest.httpArrowCapability())
	assert.True(t, dc.rest.connectionInfoInitialized)

	require.NoError(t, dc.Close())

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, loginCount)
	assert.Equal(t, 1, logoutCount)
}

func TestBuildDatabendConnFallsBackToVersionQueryWhenLoginNotFound(t *testing.T) {
	var (
		mu                sync.Mutex
		loginCount        int
		versionQueryCount int
	)

	versionResp := QueryResponse{
		ID:     "query-version",
		Schema: &[]DataField{{Name: "version", Type: "String"}},
		Data:   [][]*string{{strPtr("Databend Query v1.2.898-nightly")}},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/session/login":
			mu.Lock()
			loginCount++
			mu.Unlock()
			http.NotFound(w, r)
		case "/v1/query":
			var req QueryRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			assert.Equal(t, "SELECT version()", req.SQL)

			mu.Lock()
			versionQueryCount++
			mu.Unlock()

			w.Header().Set(contentType, jsonMediaType)
			require.NoError(t, json.NewEncoder(w).Encode(versionResp))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	cfg := testHTTPConfig(t, server.URL)
	dc, err := buildDatabendConn(context.Background(), cfg)
	require.NoError(t, err)

	assert.False(t, dc.rest.httpArrowCapability())
	assert.True(t, dc.rest.connectionInfoInitialized)
	assert.False(t, dc.rest.sessionLoggedIn)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, loginCount)
	assert.Equal(t, 1, versionQueryCount)
}

func TestBuildDatabendConnSkipsLoginWhenDisabled(t *testing.T) {
	var (
		mu                sync.Mutex
		loginCount        int
		versionQueryCount int
	)

	versionResp := QueryResponse{
		ID:     "query-version",
		Schema: &[]DataField{{Name: "version", Type: "String"}},
		Data:   [][]*string{{strPtr("Databend Query v1.2.899-nightly")}},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/session/login":
			mu.Lock()
			loginCount++
			mu.Unlock()
			http.NotFound(w, r)
		case "/v1/query":
			var req QueryRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			assert.Equal(t, "SELECT version()", req.SQL)

			mu.Lock()
			versionQueryCount++
			mu.Unlock()

			w.Header().Set(contentType, jsonMediaType)
			require.NoError(t, json.NewEncoder(w).Encode(versionResp))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	cfg := testHTTPConfig(t, server.URL)
	cfg.LoginEnabled = false
	dc, err := buildDatabendConn(context.Background(), cfg)
	require.NoError(t, err)

	assert.True(t, dc.rest.httpArrowCapability())
	assert.True(t, dc.rest.connectionInfoInitialized)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 0, loginCount)
	assert.Equal(t, 1, versionQueryCount)
}
