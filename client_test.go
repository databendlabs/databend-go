package godatabend

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
