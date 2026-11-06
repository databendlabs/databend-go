package godatabend

import (
	"bufio"
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newUploadTestClient(server *httptest.Server) *APIClient {
	return &APIClient{
		cli:                  server.Client(),
		apiEndpoint:          server.URL,
		host:                 strings.TrimPrefix(strings.TrimPrefix(server.URL, "http://"), "https://"),
		tenant:               "default",
		user:                 "root",
		sessionState:         &SessionState{},
		PresignedURLDisabled: true,
	}
}

// The streamed body is a well-formed single-part upload, with an exact
// Content-Length when the size is known and chunked otherwise.
func TestUploadToStageByAPIStreamsMultipart(t *testing.T) {
	payload := strings.Repeat("1,2,3\n", 10000)
	type seen struct {
		contentLength int64
		chunked       bool
		filename      string
		content       string
		stage         string
	}
	got := make(chan seen, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var s seen
		s.contentLength = r.ContentLength
		s.chunked = len(r.TransferEncoding) > 0 && r.TransferEncoding[0] == "chunked"
		s.stage = r.Header.Get("stage_name")
		mr, err := r.MultipartReader()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		part, err := mr.NextPart()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		_, params, _ := mime.ParseMediaType(part.Header.Get("Content-Disposition"))
		s.filename = params["filename"]
		b, _ := io.ReadAll(part)
		s.content = string(b)
		if _, err := mr.NextPart(); err != io.EOF {
			http.Error(w, "expected a single part", http.StatusBadRequest)
			return
		}
		got <- s
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client := newUploadTestClient(server)
	stage := &StageLocation{Name: "uploads", Path: "backfill/test.csv"}

	err := client.UploadToStage(context.Background(), stage,
		bufio.NewReader(strings.NewReader(payload)), int64(len(payload)))
	require.NoError(t, err)
	s := <-got
	assert.Equal(t, payload, s.content)
	assert.Equal(t, "backfill/test.csv", s.filename)
	assert.Equal(t, "uploads", s.stage)
	assert.Greater(t, s.contentLength, int64(len(payload)))
	assert.False(t, s.chunked)

	err = client.UploadToStageByAPI(context.Background(), stage,
		bufio.NewReader(strings.NewReader(payload)))
	require.NoError(t, err)
	s = <-got
	assert.Equal(t, payload, s.content)
	assert.Equal(t, int64(-1), s.contentLength)
	assert.True(t, s.chunked)
}

// A size that does not match the input fails the upload instead of sending
// a truncated or overlong body. The server only consumes the multipart part,
// as Databend does, so it is the client that must refuse the mismatch: over
// HTTP/2 Content-Length alone does not stop a well-formed body that is
// shorter than announced.
func TestUploadToStageByAPIRejectsWrongSize(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mr, err := r.MultipartReader()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		part, err := mr.NextPart()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if _, err := io.Copy(io.Discard, part); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	const payload = "1,2,3\n"
	for _, proto := range []string{"HTTP/1.1", "HTTP/2"} {
		for _, size := range []int64{int64(len(payload)) + 94, int64(len(payload)) - 2} {
			t.Run(fmt.Sprintf("%s/size=%d", proto, size), func(t *testing.T) {
				server := httptest.NewUnstartedServer(handler)
				if proto == "HTTP/2" {
					server.EnableHTTP2 = true
					server.StartTLS()
				} else {
					server.Start()
				}
				defer server.Close()
				client := newUploadTestClient(server)
				err := client.UploadToStage(context.Background(),
					&StageLocation{Name: "uploads", Path: "x.csv"},
					bufio.NewReader(strings.NewReader(payload)), size)
				require.Error(t, err)
			})
		}
	}
}

// An exact size uploads successfully over HTTP/2 too.
func TestUploadToStageByAPIExactSizeHTTP2(t *testing.T) {
	const payload = "1,2,3\n"
	got := make(chan string, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mr, err := r.MultipartReader()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		part, err := mr.NextPart()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		b, _ := io.ReadAll(part)
		got <- r.Proto + " " + string(b)
		w.WriteHeader(http.StatusOK)
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	client := newUploadTestClient(server)
	err := client.UploadToStage(context.Background(),
		&StageLocation{Name: "uploads", Path: "x.csv"},
		bufio.NewReader(strings.NewReader(payload)), int64(len(payload)))
	require.NoError(t, err)
	assert.Equal(t, "HTTP/2.0 "+payload, <-got)
}

// A redirected upload never reaches its target: Go does not replay a streamed
// body on 307/308 and turns a PUT into a body-less GET on 301/302/303. Either
// way the upload must fail rather than report success.
func TestUploadToStageByAPIRejectsRedirect(t *testing.T) {
	for _, code := range []int{301, 302, 303, 307, 308} {
		for _, size := range []int64{6, -1} {
			t.Run(fmt.Sprintf("%d/size=%d", code, size), func(t *testing.T) {
				mux := http.NewServeMux()
				mux.HandleFunc("/v1/upload_to_stage", func(w http.ResponseWriter, r *http.Request) {
					http.Redirect(w, r, "/elsewhere", code)
				})
				mux.HandleFunc("/elsewhere", func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.Copy(io.Discard, r.Body)
					w.WriteHeader(http.StatusOK)
				})
				server := httptest.NewServer(mux)
				defer server.Close()
				client := newUploadTestClient(server)
				err := client.uploadToStageByAPI(context.Background(),
					&StageLocation{Name: "uploads", Path: "x.csv"},
					strings.NewReader("1,2,3\n"), size)
				require.Error(t, err)
			})
		}
	}
}

// Memory does not grow with the upload: the body used to be assembled in a
// bytes.Buffer, so a multi-GB file cost multi-GB of heap.
func TestUploadToStageByAPIDoesNotBuffer(t *testing.T) {
	const size = 64 << 20
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client := newUploadTestClient(server)
	input := io.LimitReader(zeroReader{}, size)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	err := client.UploadToStage(context.Background(),
		&StageLocation{Name: "uploads", Path: "big.csv"},
		bufio.NewReader(input), size)
	require.NoError(t, err)
	runtime.ReadMemStats(&after)
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(size/4))
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func TestUploadToStageByAPIHonorsContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(250 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := &APIClient{
		cli:          server.Client(),
		apiEndpoint:  server.URL,
		host:         strings.TrimPrefix(server.URL, "http://"),
		tenant:       "default",
		user:         "root",
		sessionState: &SessionState{},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	started := time.Now()
	err := client.UploadToStageByAPI(
		ctx,
		&StageLocation{Name: "uploads", Path: "backfill/test.csv"},
		bufio.NewReader(strings.NewReader("1,2,3\n")),
	)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(started), 200*time.Millisecond)
}

func TestMakeHeadersUserPassword(t *testing.T) {
	c := APIClient{
		user:         "root",
		password:     "root",
		host:         "localhost:8000",
		tenant:       "default",
		sessionState: &SessionState{Role: "role1"},
	}
	headers, err := c.makeHeaders(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, []string{"Basic cm9vdDpyb290"}, headers["Authorization"])
	assert.Equal(t, []string{"default"}, headers["X-Databend-Tenant"])
	session := c.getSessionState()
	assert.Equal(t, "role1", session.Role)
}

func TestMakeHeadersAccessToken(t *testing.T) {
	c := APIClient{
		host:              "tn3ftqihs--bl.ch.aws-us-east-2.default.databend.com",
		tenant:            "tn3ftqihs",
		accessTokenLoader: NewStaticAccessTokenLoader("abc123"),
		warehouse:         "small-abc",
	}
	ctx := checkQueryID(context.Background())
	headers, err := c.makeHeaders(ctx)
	assert.NoError(t, err)
	assert.Equal(t, []string{"Bearer abc123"}, headers["Authorization"])
	assert.Equal(t, []string{"tn3ftqihs"}, headers["X-Databend-Tenant"])
	assert.Equal(t, []string{"small-abc"}, headers["X-Databend-Warehouse"])
	assert.NotEmptyf(t, headers["X-Databend-Query-Id"], "Query ID is not empty")
}

func TestCheckQueryID(t *testing.T) {
	// Test case 1: Context does not have ContextKeyQueryID
	ctx := context.Background()
	newCtx := checkQueryID(ctx)
	_, ok := newCtx.Value(ContextKeyQueryID).(string)
	assert.True(t, ok, "Expected ContextKeyQueryID to be present in the context")

	// Test case 2: Context already has ContextKeyQueryID
	queryID := uuid.NewString()
	ctxWithQueryID := context.WithValue(ctx, ContextKeyQueryID, queryID)
	newCtxWithQueryID := checkQueryID(ctxWithQueryID)
	assert.Equal(t, queryID, newCtxWithQueryID.Value(ContextKeyQueryID), "Expected ContextKeyQueryID to remain unchanged in the context")
}

func TestMakeHeadersQueryID(t *testing.T) {
	c := APIClient{
		user:         "root",
		password:     "root",
		host:         "localhost:8000",
		tenant:       "default",
		sessionState: &SessionState{Role: "role1"},
	}
	queryId := uuid.NewString()
	ctx := context.WithValue(context.Background(), ContextKeyQueryID, queryId)
	headers, err := c.makeHeaders(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{queryId}, headers["X-Databend-Query-Id"])
}

func TestDoQuery(t *testing.T) {
	var result QueryResponse
	result.ID = "mockid1"
	result.Stats = new(QueryStats)
	mockDoRequest := func(method, path string, req interface{}, resp interface{}) error {
		buf, _ := json.Marshal(result)
		_ = json.Unmarshal(buf, resp)
		return nil
	}

	var gotQueryID string
	statsTracker := func(queryID string, stats *QueryStats) {
		gotQueryID = queryID
	}

	c := APIClient{
		host:              "tnxxxxxxx.gw.aws-us-east-2.default.databend.com",
		tenant:            "tnxxxxxxx",
		accessTokenLoader: NewStaticAccessTokenLoader("abc123"),
		warehouse:         "small-abc",
		doRequestFunc:     mockDoRequest,
		statsTracker:      statsTracker,
	}
	queryId := "mockid1"
	ctx := context.WithValue(context.Background(), ContextKeyQueryID, queryId)
	resp, err := c.StartQuery(ctx, "SELECT 1")
	assert.NoError(t, err)
	assert.Equal(t, "mockid1", gotQueryID)
	assert.Equal(t, resp.ID, queryId)
}

func TestMakeHeadersUserAgent(t *testing.T) {
	pkgVersion := strings.TrimSpace(version)

	// case 1: no userAgent set, default header
	c := APIClient{
		user:         "root",
		password:     "root",
		host:         "localhost:8000",
		sessionState: &SessionState{},
	}
	headers, err := c.makeHeaders(context.Background())
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("databend-go/%s", pkgVersion), headers.Get("User-Agent"))

	// case 2: userAgent set via config field
	c.userAgent = "grafana-databend-datasource"
	headers, err = c.makeHeaders(context.Background())
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("databend-go/%s (grafana-databend-datasource)", pkgVersion), headers.Get("User-Agent"))

	// case 3: context overrides config field
	ctx := context.WithValue(context.Background(), ContextUserAgentID, "bendsql")
	headers, err = c.makeHeaders(ctx)
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("databend-go/%s (bendsql)", pkgVersion), headers.Get("User-Agent"))
}

func TestClientStateRoundTripRestoresQuerySeq(t *testing.T) {
	c := APIClient{
		SessionID:    "session-1",
		QuerySeq:     7,
		routeHint:    "route-1",
		nodeID:       "node-1",
		sessionState: &SessionState{Settings: map[string]string{"max_result_rows": "5"}},
	}
	sessionStateRaw, err := json.Marshal(c.sessionState)
	require.NoError(t, err)
	raw := json.RawMessage(sessionStateRaw)
	c.sessionStateRaw = &raw
	c.cli = NewAPIHttpClientFromConfig(NewConfig())
	c.cli.Jar.SetCookies(nil, []*http.Cookie{{Name: "session_id", Value: "cookie-session"}})

	state := c.GetState()
	require.NotNil(t, state)
	assert.Equal(t, int64(7), state.QuerySeq)

	restored := NewAPIClientFromConfig(NewConfig())
	restored.WithState(state)
	assert.Equal(t, "session-1", restored.SessionID)
	assert.Equal(t, int64(7), restored.QuerySeq)

	restored.NextQuery()
	assert.Equal(t, "session-1.8", restored.GetQueryID())
}

// A redirected presigned upload never reaches its target: Go does not replay a
// streamed body on 307/308 and turns a PUT into a body-less GET on
// 301/302/303. Either way the upload must fail rather than report success.
func TestUploadToPresignedURLRejectsRedirect(t *testing.T) {
	for _, code := range []int{301, 302, 303, 307, 308} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, "/elsewhere", code)
			})
			mux.HandleFunc("/elsewhere", func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.WriteHeader(http.StatusOK)
			})
			server := httptest.NewServer(mux)
			defer server.Close()
			client := &APIClient{cli: server.Client()}
			err := client.uploadToPresignedURL(context.Background(),
				&PresignedResponse{Method: http.MethodPut, URL: server.URL + "/upload"},
				bufio.NewReader(strings.NewReader("1,2,3\n")), 6)
			require.Error(t, err)
		})
	}
}

// S3 answers a request to the wrong regional endpoint with a 301 that has no
// Location header, which Go returns as the final response.
func TestUploadToPresignedURLRejectsRedirectWithoutLocation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusMovedPermanently)
	}))
	defer server.Close()
	client := &APIClient{cli: server.Client()}
	err := client.uploadToPresignedURL(context.Background(),
		&PresignedResponse{Method: http.MethodPut, URL: server.URL + "/upload"},
		bufio.NewReader(strings.NewReader("1,2,3\n")), 6)
	require.Error(t, err)
}

func TestUploadToPresignedURL(t *testing.T) {
	var got string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = r.Method + " " + r.Header.Get("X-Amz-Test") + " " + string(b)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client := &APIClient{cli: server.Client()}
	err := client.uploadToPresignedURL(context.Background(),
		&PresignedResponse{Method: http.MethodPut, URL: server.URL + "/upload", Headers: map[string]string{"X-Amz-Test": "v"}},
		bufio.NewReader(strings.NewReader("1,2,3\n")), 6)
	require.NoError(t, err)
	assert.Equal(t, "PUT v 1,2,3\n", got)
}

// After a Databend node is replaced, requests that still carry the old node id
// in X-DATABEND-STICKY-NODE are rejected with 400 "Sticky session state lost".
// The client must drop the stale node id and the session flags that forced
// stickiness (need_sticky, internal.has_temp_table), stop sending the header,
// and report the wrapping connection invalid so database/sql discards it.
// Otherwise a pooled connection keeps failing on every statement forever.
func TestStickyNodeLostResetsClientState(t *testing.T) {
	var seenStickyNode []string
	var seenSession []map[string]json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/query", r.URL.Path)
		var req QueryRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		var session map[string]json.RawMessage
		if req.Session != nil {
			require.NoError(t, json.Unmarshal(*req.Session, &session))
		}
		seenSession = append(seenSession, session)

		sticky := r.Header.Get(DatabendQueryStickyNode)
		seenStickyNode = append(seenStickyNode, sticky)
		w.Header().Set("Content-Type", "application/json")
		if sticky != "" {
			w.WriteHeader(http.StatusBadRequest)
			// Real shape: json_response rewrites the middleware error as code=<http status>,
			// message=<poem err.to_string()>, with get_request_info appended.
			_, _ = fmt.Fprintf(w, `{"error":{"code":400,"message":"Some(400) BadArguments. Code: 1006, Text = Sticky session state lost: node '%s' not found in cluster, request=method=POST uri=/v1/query headers={\"authorization\": \"Basic SECRET\"}"}}`, sticky)
			return
		}
		_, _ = io.WriteString(w, `{"id":"q2","node_id":"new-node","state":"Succeeded","schema":[],"data":[],"stats":{},"session":{"need_sticky":false}}`)
	}))
	defer server.Close()

	var logBuf bytes.Buffer
	logger.SetOutput(&logBuf)
	defer logger.SetOutput(os.Stdout)

	c := newUploadTestClient(server)
	c.SessionID = "client-sess"
	c.cli.Jar = NewIgnoreDomainCookieJar()
	c.cli.Jar.SetCookies(nil, []*http.Cookie{{Name: "session_id", Value: "server-sess"}})
	c.nodeID = "old-node"
	c.sessionState = &SessionState{NeedSticky: true, NeedKeepAlive: true}
	raw := json.RawMessage(`{"settings":{"max_threads":"4"},"need_sticky":true,"need_keep_alive":true,"internal":{"has_temp_table":true,"last_node_id":"old-node"}}`)
	c.sessionStateRaw = &raw

	_, err := c.startQuery(context.Background(), "select 1", queryTransportJSON)
	require.Error(t, err)
	assert.True(t, IsSessionInvalidated(err), "got: %v", err)
	require.Len(t, seenStickyNode, 1)
	assert.Equal(t, "old-node", seenStickyNode[0])

	assertSessionReset(t, c)

	// Further queries are refused locally; nothing else reaches the server.
	// The refusal keeps the original cause and the ids for diagnosis.
	_, err = c.startQuery(context.Background(), "select 1", queryTransportJSON)
	assert.ErrorIs(t, err, ErrSessionInvalidated)
	var inv *SessionInvalidatedError
	require.ErrorAs(t, err, &inv)
	assert.Equal(t, "old-node", inv.LostNodeID)
	assert.Equal(t, "server-sess", inv.SessionID, "cookie wins over the client-generated id")
	assert.Contains(t, err.Error(), "Sticky session state lost: node 'old-node' not found in cluster")
	assert.NotContains(t, err.Error(), "request=", "header dump is dropped from the stored cause")
	// The one log line carries the ids and the short cause, never the header dump.
	assert.Contains(t, logBuf.String(), "session_id=server-sess lost_node_id=old-node")
	assert.Contains(t, logBuf.String(), "not found in cluster")
	assert.NotContains(t, logBuf.String(), "SECRET", "server header dump must not reach application logs")
	assert.Contains(t, err.Error(), "lost_node_id=old-node")
	assert.True(t, IsSessionInvalidated(err), "cause stays reachable through Unwrap")
	_, err = c.pollQueryWithTransport(context.Background(), "/v1/query/q1/page/1", queryTransportJSON)
	assert.ErrorIs(t, err, ErrSessionInvalidated)
	assert.Len(t, seenStickyNode, 1)
	assert.Len(t, seenSession, 1)
}

// The same session state goes stale without any node change: a MATERIALIZED
// CTE creates server-side temp tables for the duration of one query, and an
// intermediate page response reported them as session temp tables. Once the
// query finished and dropped them, every following statement was rejected
// with SessionTimeout (5110). Unlike the sticky case this comes back as
// HTTP 200 with the error in the body, and the body echoes the stale session.
func TestSessionTimeoutInBodyResetsClientState(t *testing.T) {
	var seenSession []map[string]json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req QueryRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		var session map[string]json.RawMessage
		if req.Session != nil {
			require.NoError(t, json.Unmarshal(*req.Session, &session))
		}
		seenSession = append(seenSession, session)
		w.Header().Set("Content-Type", "application/json")
		if _, stale := session["internal"]; stale {
			// Mirrors fail_to_start_sql: 200, state Failed, session echoed back unchanged.
			// (A real server would also drop the sticky header requirement here.)
			_, _ = fmt.Fprintf(w, `{"id":"","state":"Failed","schema":[],"data":[],"stats":{},"error":{"code":5110,"message":"temporary tables in session s1 expired after idle for more than 14400 seconds, when starting query q1"},"session":%s}`, *req.Session)
			return
		}
		_, _ = io.WriteString(w, `{"id":"q2","node_id":"n1","state":"Succeeded","schema":[],"data":[],"stats":{},"session":{}}`)
	}))
	defer server.Close()

	c := newUploadTestClient(server)
	c.nodeID = "n1"
	c.sessionState = &SessionState{NeedSticky: true, NeedKeepAlive: true}
	raw := json.RawMessage(`{"settings":{"max_threads":"4"},"need_sticky":true,"need_keep_alive":true,"internal":{"has_temp_table":true,"last_node_id":"n1"}}`)
	c.sessionStateRaw = &raw

	_, err := c.startQuery(context.Background(), "select 1", queryTransportJSON)
	require.Error(t, err)
	assert.True(t, IsSessionInvalidated(err), "got: %v", err)
	assertSessionReset(t, c)

	_, err = c.startQuery(context.Background(), "select 1", queryTransportJSON)
	assert.ErrorIs(t, err, ErrSessionInvalidated)
	assert.Len(t, seenSession, 1)
}

// A running query can fail with TransactionTimeout (4003) on a page response
// because of a fuse snapshot timestamp conflict. That is an ordinary query
// failure: the session, its temp tables and the connection are all fine.
func TestPageErrorDoesNotInvalidateSession(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/page/1") {
			_, _ = io.WriteString(w, `{"id":"q1","node_id":"n1","state":"Failed","schema":[],"data":[],"stats":{},"error":{"code":4003,"message":"Conflict window too narrow: ..."},"session":{"need_sticky":true,"need_keep_alive":true,"internal":{"has_temp_table":true,"last_node_id":"n1"}}}`)
			return
		}
		_, _ = io.WriteString(w, `{"id":"q1","node_id":"n1","state":"Running","schema":[],"data":[],"stats":{},"next_uri":"/v1/query/q1/page/1","session":{"need_sticky":true,"need_keep_alive":true,"internal":{"has_temp_table":true,"last_node_id":"n1"}}}`)
	}))
	defer server.Close()

	c := newUploadTestClient(server)
	resp, err := c.startQuery(context.Background(), "insert into t values (1)", queryTransportJSON)
	require.NoError(t, err)
	_, err = c.pollQueryWithTransport(context.Background(), resp.NextURI, queryTransportJSON)
	require.Error(t, err)
	var qe *QueryError
	require.ErrorAs(t, err, &qe)
	assert.Equal(t, 4003, qe.Code)

	assert.False(t, c.SessionInvalidated())
	assert.Equal(t, "n1", c.nodeID)
	assert.True(t, c.NeedSticky())
	assert.Contains(t, string(*c.sessionStateRaw), `"has_temp_table":true`)
	// The connection stays usable.
	_, err = c.startQuery(context.Background(), "select 1", queryTransportJSON)
	require.NoError(t, err)
}

func assertSessionReset(t *testing.T, c *APIClient) {
	t.Helper()
	assert.True(t, c.SessionInvalidated())
	assert.Empty(t, c.nodeID)
	assert.False(t, c.NeedSticky())
	assert.False(t, c.NeedKeepAlive())
	var got map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(*c.sessionStateRaw, &got))
	assert.NotContains(t, got, "need_sticky")
	assert.NotContains(t, got, "need_keep_alive")
	assert.NotContains(t, got, "internal")
	assert.JSONEq(t, `{"max_threads":"4"}`, string(got["settings"]), "unrelated session state must be preserved")

	dc := &DatabendConn{rest: c, cfg: NewConfig()}
	assert.False(t, dc.IsValid(), "pool must discard a connection whose session state is gone")
}

func TestClearStickySessionStateKeepsOtherInternalFields(t *testing.T) {
	// An active transaction is marked Fail so the server keeps rejecting the
	// remaining statements with 4002 even on a client restored via WithState.
	c := &APIClient{sessionState: &SessionState{TxnState: TxnStateActive}}
	raw := json.RawMessage(`{"role":"r","txn_state":"Active","internal":{"has_temp_table":true,"last_node_id":"n","variables":{"a":"1"}}}`)
	c.sessionStateRaw = &raw
	c.clearStickySessionState()
	assert.Equal(t, TxnStateFail, c.sessionState.TxnState)
	assert.JSONEq(t, `{"role":"r","txn_state":"Fail","internal":{"variables":{"a":"1"}}}`, string(*c.sessionStateRaw))

	// Other txn states and a server-provided Fail are left alone.
	for _, st := range []TxnState{TxnStateAutoCommit, TxnStateFail} {
		c := &APIClient{sessionState: &SessionState{TxnState: st}}
		raw := json.RawMessage(`{"txn_state":"` + string(st) + `","need_sticky":true}`)
		c.sessionStateRaw = &raw
		c.clearStickySessionState()
		assert.Equal(t, st, c.sessionState.TxnState)
		assert.JSONEq(t, `{"txn_state":"`+string(st)+`"}`, string(*c.sessionStateRaw))
	}
}

func TestIsSessionInvalidated(t *testing.T) {
	// Shape sent by current servers (json_response: code = HTTP status, message = poem error text).
	sticky := []byte(`{"error":{"code":400,"message":"Some(400) BadArguments. Code: 1006, Text = Sticky session state lost: node 'n1' not found in cluster, request=method=POST uri=/v1/query headers={}"}}`)
	assert.True(t, IsSessionInvalidated(NewAPIError("please check your arguments", 400, sticky)))
	assert.True(t, IsSessionInvalidated(fmt.Errorf("wrapped: %w", NewAPIError("x", 400, sticky))))
	assert.True(t, IsSessionInvalidated(&QueryError{Code: 1006, Message: "Sticky session state lost: node 'n1' not found in cluster"}))
	assert.True(t, IsSessionInvalidated(&QueryError{Code: 400, Message: "Some(400) BadArguments. Code: 1006, Text = Sticky session state lost: node 'n1' not found in cluster"}))
	// The message alone is not enough: error text can embed user SQL.
	assert.False(t, IsSessionInvalidated(&QueryError{Code: 1065, Message: "error: select 'Sticky session state lost' from t"}))
	// New servers report the sticky rejection with its own code, 400 body or query body.
	assert.True(t, IsSessionInvalidated(NewAPIError("x", 400, []byte(`{"error":{"code":5113,"message":"node 'n1' not found in cluster"}}`))),
		"5113 must be matched by code even if the text changes")
	assert.True(t, IsSessionInvalidated(&QueryError{Code: 5113, Message: "whatever"}))
	// 4003: an idle-expired transaction on a still-valid node (server returns
	// txn_state=Fail, ROLLBACK recovers) or a fuse snapshot conflict. Neither
	// means the session is lost.
	assert.False(t, IsSessionInvalidated(&QueryError{Code: 4003, Message: "Transaction timeout: last_query_id q1 not found on this server"}))
	assert.False(t, IsSessionInvalidated(&QueryError{Code: 4003, Message: "Conflict window too narrow: ..."}))
	for _, code := range []int{5110, 5111, 5112, 5113} {
		assert.True(t, IsSessionInvalidated(&QueryError{Code: code, Message: "x"}), "code %d", code)
		assert.True(t, IsSessionInvalidated(fmt.Errorf("query error: %w", &QueryError{Code: code})), "wrapped code %d", code)
	}

	assert.False(t, IsSessionInvalidated(nil))
	assert.False(t, IsSessionInvalidated(NewAPIError("x", 500, sticky)), "only a 4xx rejection means the node is gone")
	assert.False(t, IsSessionInvalidated(NewAPIError("x", 400, []byte(`{"error":{"code":1006,"message":"other bad argument"}}`))))
	assert.False(t, IsSessionInvalidated(NewAPIError("x", 400, []byte(`not json`))))
	assert.False(t, IsSessionInvalidated(NewAPIError("x", 500, []byte(`{"error":{"code":5113,"message":"x"}}`))), "5xx is a server fault, not a stale session")
	assert.False(t, IsSessionInvalidated(&QueryError{Code: 1006, Message: "other"}))
	// Aborted transaction is recoverable with ROLLBACK; server sets txn_state=Fail for it.
	assert.False(t, IsSessionInvalidated(&QueryError{Code: 4002, Message: "Current transaction is aborted"}))
	assert.False(t, IsSessionInvalidated(&QueryError{Code: 1025, Message: "Unknown table"}))
}

// Inside a transaction the server-side state loss is fatal for the whole
// transaction. After the failing statement, later statements are refused
// locally with one clear error instead of a server-side 4002 round trip, and
// Commit must fail: the server would answer COMMIT in Fail state with success
// without committing anything.
func TestTransactionAfterSessionLostIsRefused(t *testing.T) {
	var sqls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req QueryRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		sqls = append(sqls, req.SQL)
		w.Header().Set("Content-Type", "application/json")
		switch req.SQL {
		case "BEGIN":
			_, _ = io.WriteString(w, `{"id":"q1","node_id":"n1","state":"Succeeded","schema":[],"data":[],"stats":{},"session":{"txn_state":"Active","need_sticky":true,"need_keep_alive":true,"internal":{"last_node_id":"n1"}}}`)
		default:
			// n1 restarted: check_sticky_for_txn -> SessionLost, session echoed back.
			_, _ = fmt.Fprintf(w, `{"id":"","state":"Failed","schema":[],"data":[],"stats":{},"error":{"code":5111,"message":"Transaction aborted because server restart or route error: expecting server n1, current one is n2"},"session":%s}`, *req.Session)
		}
	}))
	defer server.Close()

	c := newUploadTestClient(server)
	c.cli.Jar = NewIgnoreDomainCookieJar()
	raw := json.RawMessage(`{}`)
	c.sessionStateRaw = &raw
	dc := &DatabendConn{ctx: context.Background(), cfg: NewConfig(), rest: c}

	tx, err := dc.BeginTx(context.Background(), driver.TxOptions{})
	require.NoError(t, err)
	assert.True(t, c.inActiveTransaction())

	_, err = dc.ExecContext(context.Background(), "INSERT INTO b VALUES (1)", nil)
	require.Error(t, err)
	assert.True(t, IsSessionInvalidated(err), "got: %v", err)

	_, err = dc.ExecContext(context.Background(), "INSERT INTO c VALUES (1)", nil)
	assert.ErrorIs(t, err, driver.ErrBadConn)
	assert.ErrorIs(t, err, ErrSessionInvalidated, "mapped error keeps the cause")
	assert.Contains(t, err.Error(), "expecting server n1")
	_, err = dc.QueryContext(context.Background(), "SELECT 1", nil)
	assert.ErrorIs(t, err, driver.ErrBadConn)
	_, err = dc.ExecBatch(context.Background(), "INSERT INTO c VALUES (?)", [][]driver.Value{{1}})
	assert.ErrorIs(t, err, driver.ErrBadConn)
	// BEGIN and the failing INSERT were sent in this checkout: the caller
	// evidently holds the connection.
	assert.Contains(t, err.Error(), "stmts=2")
	// /v1/verify alone would say the connection is fine.
	assert.ErrorIs(t, dc.Ping(context.Background()), driver.ErrBadConn)
	err = tx.Commit()
	assert.ErrorIs(t, err, driver.ErrBadConn)
	assert.ErrorIs(t, err, ErrSessionInvalidated)
	assert.NoError(t, tx.Rollback())
	assert.False(t, dc.IsValid())
	assert.Equal(t, 0, c.checkoutStmts, "IsValid marks the end of a checkout")

	assert.Equal(t, []string{"BEGIN", "INSERT INTO b VALUES (1)"}, sqls, "nothing after the failure may reach the server")

	// The local flag does not survive GetState/WithState, but txn_state=Fail
	// does, so the server still refuses the rest of the "transaction".
	restored := NewAPIClientFromConfig(NewConfig()).WithState(c.GetState())
	assert.False(t, restored.SessionInvalidated())
	assert.Equal(t, TxnStateFail, restored.sessionState.TxnState)
	assert.Empty(t, restored.nodeID)
	assert.False(t, restored.NeedSticky())
}

// The Arrow transport goes through doRequestRaw, which has its own 4xx branch.
func TestStickyNodeLostArrowTransport(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		require.Equal(t, arrowStreamContentType, r.Header.Get("Accept"))
		require.Equal(t, "old-node", r.Header.Get(DatabendQueryStickyNode))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":{"code":400,"message":"Some(400) BadArguments. Code: 1006, Text = Sticky session state lost: node 'old-node' not found in cluster, request=method=POST uri=/v1/query headers={}"}}`)
	}))
	defer server.Close()

	c := newUploadTestClient(server)
	c.nodeID = "old-node"
	c.sessionState = &SessionState{NeedSticky: true, NeedKeepAlive: true}
	raw := json.RawMessage(`{"settings":{"max_threads":"4"},"need_sticky":true,"internal":{"has_temp_table":true,"last_node_id":"old-node"}}`)
	c.sessionStateRaw = &raw

	_, err := c.startQuery(context.Background(), "select 1", queryTransportArrow)
	require.Error(t, err)
	assert.True(t, IsSessionInvalidated(err), "got: %v", err)
	assert.Equal(t, 1, requests, "doRetry must not retry a 400")
	assertSessionReset(t, c)

	_, err = c.startQuery(context.Background(), "select 1", queryTransportArrow)
	assert.ErrorIs(t, err, ErrSessionInvalidated)
	assert.Equal(t, 1, requests)
}
